use std::{io, ops::Deref, path::Path, sync::Arc};

use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::oneshot,
};

#[derive(thiserror::Error, Debug)]
pub enum FetcherError {
    #[error("block offset out of range")]
    InvalidBlockOffset,
    #[error("chunk offset out of range")]
    InvalidChunkOffset,
    #[error("chunk size out of range")]
    InvalidChunkSize,
    #[error("file handler is dead")]
    FileHandlerStopped,
    #[error("error while operating on file: {0}")]
    IoError(#[source] io::Error),
}

#[derive(Debug)]
pub struct Block {
    index: u64,
    data: Vec<u8>,
}

impl Block {
    pub fn new(index: u64, data: Vec<u8>) -> Self {
        Block { index, data }
    }
}

pub struct Chunk {
    index: u64,
    begin: u64,
    length: u64,
}

impl Chunk {
    pub fn new(index: u64, begin: u64, length: u64) -> Self {
        Self {
            index,
            begin,
            length,
        }
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
        let fire_io_error = |e: io::Error| events.on_write_error(FetcherError::IoError(e));

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
                fire_io_error(e).await;

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
                        fire_io_error(e).await;

                        return;
                    }

                    let _ = chunk_sender.send(buff);
                }
                Operation::Write { index, data, .. } => {
                    if let Err(e) = handler.write_all(&data).await {
                        fire_io_error(e).await;

                        return;
                    }

                    events.on_write(index).await;
                }
                _ => unreachable!(),
            }
        }

        if let Err(e) = handler.flush().await {
            fire_io_error(e).await;
        }
    });
}

#[derive(Debug)]
pub struct FileHandler {
    operations_sender: flume::Sender<Operation>,
}

impl FileHandler {
    fn new(operations_sender: flume::Sender<Operation>) -> Self {
        Self { operations_sender }
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

    async fn send_op(&self, op: Operation) -> Result<(), flume::SendError<Operation>> {
        self.operations_sender.send_async(op).await
    }

    async fn read_chunk(&self, offset: u64, length: u64) -> Option<Vec<u8>> {
        let (chunk_sender, chunk_receiver) = oneshot::channel();

        let op = Operation::Read {
            offset,
            length,
            chunk_sender,
        };
        self.send_op(op).await.ok()?;

        chunk_receiver.await.ok()
    }

    async fn request_write_block(&self, index: u64, offset: u64, data: Vec<u8>) -> bool {
        let op = Operation::Write {
            index,
            offset,
            data,
        };
        self.send_op(op).await.is_ok()
    }

    async fn request_stop(&self) -> bool {
        self.send_op(Operation::Stop).await.is_ok()
    }
}

#[derive(Debug)]
pub struct FetcherInner {
    handler: FileHandler,
    file_size: u64,
    block_size: u64,
}

impl FetcherInner {
    fn block_offset(&self, index: u64) -> Result<u64, FetcherError> {
        let (offset, overflow) = index.overflowing_mul(self.block_size);
        if overflow || offset >= self.file_size {
            return Err(FetcherError::InvalidBlockOffset);
        }

        Ok(offset)
    }

    fn chunk_offset(&self, index: u64, begin: u64) -> Result<u64, FetcherError> {
        let block_offset = self.block_offset(index)?;

        let (offset, overflow) = block_offset.overflowing_add(begin);
        if overflow || offset >= self.file_size {
            return Err(FetcherError::InvalidChunkOffset);
        }

        Ok(offset)
    }

    pub async fn read_chunk(&self, chunk: Chunk) -> Result<Vec<u8>, FetcherError> {
        let offset = self.chunk_offset(chunk.index, chunk.begin)?;

        let (end, overflow) = offset.overflowing_add(chunk.length);
        if overflow || end > self.file_size {
            return Err(FetcherError::InvalidChunkSize);
        }

        self.handler
            .read_chunk(offset, chunk.length)
            .await
            .ok_or(FetcherError::FileHandlerStopped)
    }

    pub async fn request_block_write(&self, block: Block) -> Result<(), FetcherError> {
        let offset = self.block_offset(block.index)?;

        self.handler
            .request_write_block(block.index, offset, block.data)
            .await
            .then_some(())
            .ok_or(FetcherError::FileHandlerStopped)
    }

    pub async fn request_stop(&self) -> bool {
        self.handler.request_stop().await
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
        block_size: u64,
        events: Arc<E>,
    ) -> Result<Self, FetcherError>
    where
        E: FetcherEvents,
    {
        let handler = FileHandler::create(&path, file_size, events)
            .await
            .map_err(FetcherError::IoError)?;

        let inner = Arc::new(FetcherInner {
            handler,
            file_size,
            block_size,
        });

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
        spawn_file_handler, Block, Chunk, Fetcher, FetcherError, FetcherEvents, Operation,
    };

    fn gen_random_filename() -> String {
        format!("inner/bytes_file_{}.txt", rand::random::<u64>())
    }

    async fn create_fetcher<E>(events: Arc<E>) -> Fetcher
    where
        E: FetcherEvents,
    {
        let path = env::temp_dir().join(gen_random_filename());

        Fetcher::create(&path, 64, 32, events)
            .await
            .expect("Failed to create file fetcher")
    }

    async fn create_fetcher_without_event_handlers() -> Fetcher {
        struct EventsMock;
        impl FetcherEvents for EventsMock {}

        let events = Arc::new(EventsMock);

        let path = env::temp_dir().join(gen_random_filename());

        Fetcher::create(&path, 64, 32, events)
            .await
            .expect("Failed to create file fetcher")
    }

    #[tokio::test]
    async fn creates_a_file() {
        struct EventsMock;
        impl FetcherEvents for EventsMock {}

        let events = Arc::new(EventsMock);

        let path = env::temp_dir().join(gen_random_filename());

        assert_ok!(Fetcher::create(&path, 64, 32, events).await);

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

            let err = assert_err!(Fetcher::create(&invalid_path, 64, 32, events).await);
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

        let fetcher = create_fetcher(events).await;

        let block = Block::new(0, vec![1; 32]);
        assert_ok!(fetcher.request_block_write(block).await);
        recv_index_write_confirmation(&index_receiver, 0).await;

        let chunk = Chunk::new(0, 0, 32);
        let data = assert_ok!(fetcher.read_chunk(chunk).await);
        assert!(data.iter().all(|&b| b == 1));

        let chunk = Chunk::new(1, 0, 32);
        let data = assert_ok!(fetcher.read_chunk(chunk).await);
        assert!(data.iter().all(|&b| b == 0));

        let block = Block::new(1, vec![2; 32]);
        assert_ok!(fetcher.request_block_write(block).await);
        recv_index_write_confirmation(&index_receiver, 1).await;

        let chunk = Chunk::new(1, 0, 32);
        let data = assert_ok!(fetcher.read_chunk(chunk).await);
        assert!(data.iter().all(|&b| b == 2));

        let mut data = vec![3; 16];
        data.append(&mut vec![4; 16]);
        let block = Block::new(0, data);
        assert_ok!(fetcher.request_block_write(block).await);
        recv_index_write_confirmation(&index_receiver, 0).await;

        let chunk = Chunk::new(0, 8, 16);
        let data = assert_ok!(fetcher.read_chunk(chunk).await);
        assert!(data[..8].iter().all(|&b| b == 3));
        assert!(data[8..].iter().all(|&b| b == 4));
    }

    #[tokio::test]
    async fn fail_to_read_with_invalid_block_offset() {
        let fetcher = create_fetcher_without_event_handlers().await;

        let invalid_chunks = vec![Chunk::new(u64::MAX, 0, 32), Chunk::new(2, 0, 32)];

        for invalid_chunk in invalid_chunks {
            let err = assert_err!(fetcher.read_chunk(invalid_chunk).await);
            assert_matches!(err, FetcherError::InvalidBlockOffset);
        }
    }

    #[tokio::test]
    async fn fail_to_write_with_invalid_block_offset() {
        let fetcher = create_fetcher_without_event_handlers().await;

        let invalid_blocks = vec![
            Block::new(u64::MAX, vec![1; 32]),
            Block::new(2, vec![1; 32]),
        ];

        for invalid_block in invalid_blocks {
            let err = assert_err!(fetcher.request_block_write(invalid_block).await);
            assert_matches!(err, FetcherError::InvalidBlockOffset);
        }
    }

    #[tokio::test]
    async fn fail_to_read_with_invalid_chunk_offset() {
        let fetcher = create_fetcher_without_event_handlers().await;

        let invalid_chunks = vec![Chunk::new(1, u64::MAX, 32), Chunk::new(1, 64, 32)];

        for invalid_chunk in invalid_chunks {
            let err = assert_err!(fetcher.read_chunk(invalid_chunk).await);
            assert_matches!(err, FetcherError::InvalidChunkOffset);
        }
    }

    #[tokio::test]
    async fn fail_to_read_when_chunk_exceeds_limits() {
        let fetcher = create_fetcher_without_event_handlers().await;

        let invalid_chunks = vec![Chunk::new(1, 0, u64::MAX), Chunk::new(1, 0, 65)];

        for invalid_chunk in invalid_chunks {
            let err = assert_err!(fetcher.read_chunk(invalid_chunk).await);
            assert_matches!(err, FetcherError::InvalidChunkSize);
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
            .unwrap_or_else(|_| panic!("Didn't receive any error"))
            .unwrap();

        assert_matches!(error, FetcherError::IoError(e)
            if e.kind() == io::ErrorKind::UnexpectedEof);

        assert!(operations_sender.is_disconnected());
    }

    #[tokio::test]
    async fn fail_to_read_and_write_when_file_handler_is_stopped() {
        let fetcher = create_fetcher_without_event_handlers().await;

        assert!(fetcher.request_stop().await);

        tokio::time::sleep(Duration::from_secs(2)).await;

        let chunk = Chunk::new(1, 0, 32);
        assert_matches!(
            fetcher.read_chunk(chunk).await,
            Err(FetcherError::FileHandlerStopped)
        );

        let block = Block::new(1, vec![2; 32]);
        assert_matches!(
            fetcher.request_block_write(block).await,
            Err(FetcherError::FileHandlerStopped)
        );

        assert!(!fetcher.request_stop().await);
    }
}
