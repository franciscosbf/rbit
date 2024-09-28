use std::{
    io,
    ops::Deref,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::oneshot,
};

#[derive(thiserror::Error, Debug)]
pub enum FetcherError {
    #[error("offset out of range")]
    InvalidOffset,
    #[error("file handler is dead")]
    FileHandlerStopped,
    #[error("error while operating on file: {0}")]
    IoError(#[source] io::Error),
}

#[derive(Debug)]
pub struct Segment {
    index: u64,
    offset: u64,
    data: Vec<u8>,
}

impl Segment {
    pub fn new(index: u64, offset: u64, data: Vec<u8>) -> Self {
        Segment {
            index,
            offset,
            data,
        }
    }
}

pub struct Chunk {
    offset: u64,
    length: u64,
}

impl Chunk {
    pub fn new(offset: u64, length: u64) -> Self {
        Self { offset, length }
    }
}

#[derive(Debug)]
enum Operation {
    Read {
        offset: u64,
        length: u64,
        chunk_sender: oneshot::Sender<Vec<u8>>,
    },
    Write {
        index: u64,
        offset: u64,
        data: Vec<u8>,
    },
    Stop,
}

#[trait_variant::make(Send + Sync)]
pub trait FetcherEvents: 'static {
    async fn on_write(&self, _index: u64) {
        async {}
    }

    async fn on_write_error(&self, _error: FetcherError) {
        async {}
    }
}

fn spawn_file_handler<E>(
    mut handler: fs::File,
    operations_receiver: flume::Receiver<Operation>,
    events: Arc<E>,
) where
    E: FetcherEvents,
{
    tokio::spawn(async move {
        let fire_event_error = |e: io::Error| events.on_write_error(FetcherError::IoError(e));

        loop {
            let op = match operations_receiver.recv_async().await {
                Ok(Operation::Stop) => break,
                Err(flume::RecvError::Disconnected) => break,
                Ok(op) => op,
            };

            let offset = match op {
                Operation::Read { offset, .. } => offset,
                Operation::Write { offset, .. } => offset,
                _ => unreachable!(),
            };
            if let Err(e) = handler.seek(io::SeekFrom::Start(offset)).await {
                fire_event_error(e).await;

                return;
            }

            match op {
                Operation::Read {
                    length,
                    chunk_sender,
                    ..
                } => {
                    let mut buff = vec![0; length as usize];
                    if let Err(e) = handler.read_exact(&mut buff).await {
                        fire_event_error(e).await;

                        return;
                    }

                    let _ = chunk_sender.send(buff);
                }
                Operation::Write { index, data, .. } => {
                    if let Err(e) = handler.write_all(&data).await {
                        fire_event_error(e).await;

                        return;
                    }

                    events.on_write(index).await;
                }
                _ => unreachable!(),
            }
        }

        if let Err(e) = handler.flush().await {
            fire_event_error(e).await;
        }
    });
}

#[derive(Debug)]
pub struct FileHandler {
    operations_sender: flume::Sender<Operation>,
    stopped: AtomicBool,
}

impl FileHandler {
    fn new(operations_sender: flume::Sender<Operation>) -> Self {
        let stopped = AtomicBool::new(false);

        Self {
            operations_sender,
            stopped,
        }
    }

    async fn create<E>(path: impl AsRef<Path>, file_size: u64, events: Arc<E>) -> io::Result<Self>
    where
        E: FetcherEvents,
    {
        let parent = path.as_ref().parent().ok_or(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid file path",
        ))?;
        fs::create_dir_all(parent).await?;

        let handler = fs::File::options()
            .read(true)
            .write(true)
            .truncate(true)
            .create(true)
            .open(&path)
            .await?;

        handler.set_len(file_size).await?;

        let (operations_sender, operations_receiver) = flume::unbounded();

        spawn_file_handler(handler, operations_receiver, events);

        let fetcher = Self::new(operations_sender);

        Ok(fetcher)
    }

    fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
    }

    fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    async fn send_op(&self, op: Operation) -> Result<(), flume::SendError<Operation>> {
        self.operations_sender
            .send_async(op)
            .await
            .inspect_err(|_| self.stop())
    }

    async fn read_chunk(&self, chunk: Chunk) -> Option<Vec<u8>> {
        if self.is_stopped() {
            return None;
        }

        let offset = chunk.offset;
        let length = chunk.length;
        let (chunk_sender, chunk_receiver) = oneshot::channel();

        let op = Operation::Read {
            offset,
            length,
            chunk_sender,
        };
        self.send_op(op).await.ok()?;

        chunk_receiver.await.inspect_err(|_| self.stop()).ok()
    }

    async fn request_write_block(&self, segment: Segment) -> bool {
        if self.is_stopped() {
            return false;
        }

        let index = segment.index;
        let offset = segment.offset;
        let data = segment.data;

        let op = Operation::Write {
            index,
            offset,
            data,
        };
        self.send_op(op).await.is_ok()
    }

    async fn request_stop(&self) -> bool {
        if self.is_stopped() {
            return false;
        }

        self.stop();

        self.send_op(Operation::Stop).await.is_ok()
    }
}

#[derive(Debug)]
pub struct FetcherInner {
    handler: FileHandler,
    file_size: u64,
}

impl FetcherInner {
    pub async fn read_chunk(&self, chunk: Chunk) -> Result<Vec<u8>, FetcherError> {
        let (pos, overflow) = chunk.offset.overflowing_add(chunk.length);
        if overflow || pos > self.file_size {
            return Err(FetcherError::InvalidOffset);
        }

        self.handler
            .read_chunk(chunk)
            .await
            .ok_or(FetcherError::FileHandlerStopped)
    }

    pub async fn request_block_write(&self, segment: Segment) -> Result<(), FetcherError> {
        let (pos, overflow) = segment.offset.overflowing_add(segment.data.len() as u64);
        if overflow || pos > self.file_size {
            return Err(FetcherError::InvalidOffset);
        }

        self.handler
            .request_write_block(segment)
            .await
            .then_some(())
            .ok_or(FetcherError::FileHandlerStopped)
    }

    pub async fn request_stop(&self) -> bool {
        self.handler.request_stop().await
    }

    pub fn is_stopped(&self) -> bool {
        self.handler.is_stopped()
    }
}

#[derive(Debug, Clone)]
pub struct Fetcher {
    inner: Arc<FetcherInner>,
}

impl Fetcher {
    pub async fn create<E>(
        path: impl AsRef<Path>,
        file_size: u64,
        events: Arc<E>,
    ) -> Result<Self, FetcherError>
    where
        E: FetcherEvents,
    {
        let handler = FileHandler::create(&path, file_size, events)
            .await
            .map_err(FetcherError::IoError)?;

        let inner = Arc::new(FetcherInner { handler, file_size });

        Ok(Self { inner })
    }
}

impl Deref for Fetcher {
    type Target = FetcherInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use core::panic;
    use std::{env, io, os::unix::fs::MetadataExt, path::Path, sync::Arc, time::Duration};

    use claims::{assert_err, assert_matches, assert_ok};
    use tokio::sync::oneshot;

    use super::{
        spawn_file_handler, Chunk, Fetcher, FetcherError, FetcherEvents, Operation, Segment,
    };

    fn gen_random_filename() -> String {
        format!("inner/bytes_file_{}.txt", rand::random::<u64>())
    }

    async fn create_fetcher_with_64_bytes_file<E>(events: Arc<E>) -> Fetcher
    where
        E: FetcherEvents,
    {
        let path = env::temp_dir().join(gen_random_filename());

        Fetcher::create(&path, 64, events)
            .await
            .expect("Failed to create file fetcher")
    }

    async fn create_fetcher_without_event_handlers_with_64_bytes_file() -> Fetcher {
        struct EventsMock;
        impl FetcherEvents for EventsMock {}

        let events = Arc::new(EventsMock);

        create_fetcher_with_64_bytes_file(events).await
    }

    #[tokio::test]
    async fn creates_a_file() {
        struct EventsMock;
        impl FetcherEvents for EventsMock {}

        let events = Arc::new(EventsMock);

        let path = env::temp_dir().join(gen_random_filename());

        assert_ok!(Fetcher::create(&path, 64, events).await);

        let meta = tokio::fs::File::open(&path)
            .await
            .expect("Failed to open file")
            .metadata()
            .await
            .expect("Failed to retrieve file metadata");

        assert_eq!(meta.size(), 64);
    }

    #[tokio::test]
    async fn fails_to_create_a_file_with_invalid_file_path() {
        struct EventsMock;
        impl FetcherEvents for EventsMock {}

        let invalid_paths = vec![Path::new("/"), Path::new("")];

        for invalid_path in invalid_paths {
            let events = Arc::new(EventsMock);

            let err = assert_err!(Fetcher::create(&invalid_path, 64, events).await);
            assert_matches!(err, FetcherError::IoError(e)
                if e.kind() == std::io::ErrorKind::InvalidInput);
        }
    }

    #[tokio::test]
    async fn write_blocks_and_read_chunks() {
        struct EventsMock {
            index_sender: flume::Sender<u64>,
        }
        impl FetcherEvents for EventsMock {
            async fn on_write(&self, index: u64) {
                let _ = self.index_sender.send_async(index).await;
            }

            async fn on_write_error(&self, error: FetcherError) {
                dbg!(error);
            }
        }

        let (index_sender, index_receiver) = flume::unbounded();
        let events = Arc::new(EventsMock { index_sender });

        async fn recv_index_write_confirmation(
            index_receiver: &flume::Receiver<u64>,
            expected_index: u64,
        ) {
            let index = tokio::time::timeout(Duration::from_secs(4), index_receiver.recv_async())
                .await
                .unwrap_or_else(|_| {
                    panic!("Didn't receive write confirmation from block index {expected_index}")
                })
                .unwrap();

            assert_eq!(index, expected_index);
        }

        let fetcher = create_fetcher_with_64_bytes_file(events).await;

        let segment = Segment::new(0, 0, vec![1; 32]);
        assert_ok!(fetcher.request_block_write(segment).await);
        recv_index_write_confirmation(&index_receiver, 0).await;

        let chunk = Chunk::new(0, 32);
        let data = assert_ok!(fetcher.read_chunk(chunk).await);
        assert!(data.iter().all(|&b| b == 1));

        let chunk = Chunk::new(32, 32);
        let data = assert_ok!(fetcher.read_chunk(chunk).await);
        assert!(data.iter().all(|&b| b == 0));

        let segment = Segment::new(1, 32, vec![2; 32]);
        assert_ok!(fetcher.request_block_write(segment).await);
        recv_index_write_confirmation(&index_receiver, 1).await;

        let chunk = Chunk::new(32, 32);
        let data = assert_ok!(fetcher.read_chunk(chunk).await);
        assert!(data.iter().all(|&b| b == 2));

        let mut data = vec![3; 16];
        data.append(&mut vec![4; 16]);
        let segment = Segment::new(0, 0, data);
        assert_ok!(fetcher.request_block_write(segment).await);
        recv_index_write_confirmation(&index_receiver, 0).await;

        let chunk = Chunk::new(8, 16);
        let data = assert_ok!(fetcher.read_chunk(chunk).await);
        assert!(data[..8].iter().all(|&b| b == 3));
        assert!(data[8..].iter().all(|&b| b == 4));
    }

    #[tokio::test]
    async fn fail_to_read_with_invalid_offset() {
        let fetcher = create_fetcher_without_event_handlers_with_64_bytes_file().await;

        let invalid_chunks = vec![
            Chunk::new(0, 65),
            Chunk::new(32, 33),
            Chunk::new(u64::MAX, 1),
        ];

        for invalid_chunk in invalid_chunks {
            let err = assert_err!(fetcher.read_chunk(invalid_chunk).await);
            assert_matches!(err, FetcherError::InvalidOffset);
        }
    }

    #[tokio::test]
    async fn fail_to_write_with_invalid_offset() {
        let fetcher = create_fetcher_without_event_handlers_with_64_bytes_file().await;

        let invalid_blocks = vec![
            Segment::new(0, 0, vec![1; 65]),
            Segment::new(1, 32, vec![1; 33]),
            Segment::new(1, u64::MAX, vec![1; 2]),
        ];

        for invalid_block in invalid_blocks {
            let err = assert_err!(fetcher.request_block_write(invalid_block).await);
            assert_matches!(err, FetcherError::InvalidOffset);
        }
    }

    #[tokio::test]
    async fn file_handler_stops_on_io_error() {
        let path = env::temp_dir().join(gen_random_filename());
        let handler = tokio::fs::File::options()
            .read(true)
            .write(true)
            .truncate(true)
            .create(true)
            .open(path)
            .await
            .expect("Failed to create file");

        struct EventsMock {
            error_sender: flume::Sender<FetcherError>,
        }
        impl FetcherEvents for EventsMock {
            async fn on_write_error(&self, error: FetcherError) {
                let _ = self.error_sender.send_async(error).await;
            }
        }

        let (error_sender, error_receiver) = flume::unbounded();
        let events = Arc::new(EventsMock { error_sender });

        let (operations_sender, operations_receiver) = flume::unbounded();

        spawn_file_handler(handler, operations_receiver, events);

        let (chunk_sender, _) = oneshot::channel();
        let _ = operations_sender
            .send_async(Operation::Read {
                offset: 0,
                length: 32,
                chunk_sender,
            })
            .await;

        let error = tokio::time::timeout(Duration::from_secs(4), error_receiver.recv_async())
            .await
            .expect("Didn't receive any error")
            .unwrap();

        assert_matches!(error, FetcherError::IoError(e)
            if e.kind() == io::ErrorKind::UnexpectedEof);

        assert!(operations_sender.is_disconnected());
    }

    #[tokio::test]
    async fn fetcher_stops_on_file_handler_error() {
        struct EventsMock;
        impl FetcherEvents for EventsMock {}

        let events = Arc::new(EventsMock);

        let path = env::temp_dir().join(gen_random_filename());

        let fetcher = Fetcher::create(&path, 64, events)
            .await
            .expect("Failed to create file fetcher");

        {
            tokio::fs::File::options()
                .write(true)
                .open(&path)
                .await
                .expect("Failed to open second file handler")
                .set_len(32)
                .await
                .expect("Failed to redefine file size");
        }

        let chunk = Chunk::new(32, 32);
        assert_matches!(
            fetcher.read_chunk(chunk).await,
            Err(FetcherError::FileHandlerStopped)
        );

        assert!(fetcher.is_stopped());

        let block = Segment::new(0, 0, vec![2; 32]);
        assert_matches!(
            fetcher.request_block_write(block).await,
            Err(FetcherError::FileHandlerStopped)
        );

        assert!(!fetcher.request_stop().await);
    }

    #[tokio::test]
    async fn fail_to_read_and_write_when_file_handler_is_stopped() {
        let fetcher = create_fetcher_without_event_handlers_with_64_bytes_file().await;

        assert!(fetcher.request_stop().await);

        let chunk = Chunk::new(0, 32);
        assert_matches!(
            fetcher.read_chunk(chunk).await,
            Err(FetcherError::FileHandlerStopped)
        );

        let block = Segment::new(1, 32, vec![2; 32]);
        assert_matches!(
            fetcher.request_block_write(block).await,
            Err(FetcherError::FileHandlerStopped)
        );

        assert!(!fetcher.request_stop().await);
    }
}
