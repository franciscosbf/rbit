use std::{
    io,
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};

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
    FileHandlerClosed,
    #[error("error while operating on file '{path}': {source}")]
    IoError {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

impl FetcherError {
    fn io_error(path: impl AsRef<Path>, source: io::Error) -> Self {
        let path = path.as_ref().into();

        Self::IoError { path, source }
    }
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
        offset: u64,
        data: Vec<u8>,
    },
}

fn spawn_file_handler(mut handler: fs::File, operations_receiver: flume::Receiver<Operation>) {
    tokio::spawn(async move {
        loop {
            let op = match operations_receiver.recv_async().await {
                Ok(op) => op,
                Err(flume::RecvError::Disconnected) => break,
            };

            let offset = match op {
                Operation::Read { offset, .. } => offset,
                Operation::Write { offset, .. } => offset,
            };
            if handler.seek(io::SeekFrom::Start(offset)).await.is_err() {
                return;
            }

            match op {
                Operation::Read {
                    length,
                    chunk_sender,
                    ..
                } => {
                    let mut buff = vec![0; length as usize];
                    if handler.read_exact(&mut buff).await.is_err() {
                        return;
                    }

                    let _ = chunk_sender.send(buff);
                }
                Operation::Write { data, .. } => {
                    if handler.write_all(&data).await.is_err() {
                        return;
                    }
                }
            }
        }

        let _ = handler.flush().await;
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

    async fn create(path: impl AsRef<Path>, file_size: u64) -> io::Result<Self> {
        let parent = path.as_ref().parent().ok_or(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid file path",
        ))?;
        fs::create_dir_all(parent).await?;

        let handler = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(true)
            .create(true)
            .open(&path)
            .await?;

        handler.set_len(file_size).await?;

        let (operations_sender, operations_receiver) = flume::unbounded();

        spawn_file_handler(handler, operations_receiver);

        let fetcher = Self::new(operations_sender);

        Ok(fetcher)
    }

    async fn read_chunk(&self, offset: u64, length: u64) -> Option<Vec<u8>> {
        let (chunk_sender, chunk_receiver) = oneshot::channel();

        let op = Operation::Read {
            offset,
            length,
            chunk_sender,
        };
        self.operations_sender.send_async(op).await.ok()?;

        chunk_receiver.await.ok()
    }

    async fn write_block(&self, offset: u64, data: Vec<u8>) -> bool {
        let op = Operation::Write { offset, data };
        self.operations_sender.send_async(op).await.is_ok()
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
            .ok_or(FetcherError::FileHandlerClosed)
    }

    pub async fn write_block(&self, block: Block) -> Result<(), FetcherError> {
        let offset = self.block_offset(block.index)?;

        self.handler
            .write_block(offset, block.data)
            .await
            .then_some(())
            .ok_or(FetcherError::FileHandlerClosed)
    }
}

#[derive(Debug, Clone)]
pub struct Fetcher {
    inner: Arc<FetcherInner>,
}

impl Fetcher {
    pub async fn create(
        path: impl AsRef<Path>,
        file_size: u64,
        block_size: u64,
    ) -> Result<Self, FetcherError> {
        let handler = FileHandler::create(&path, file_size)
            .await
            .map_err(|e| FetcherError::io_error(&path, e))?;

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
    use std::{env, os::unix::fs::MetadataExt, path::Path};

    use claims::{assert_err, assert_matches, assert_ok};

    use super::{Block, Chunk, Fetcher, FetcherError};

    fn gen_random_filename() -> String {
        format!("inner/bytes_file_{}.txt", rand::random::<u64>())
    }

    async fn create_random_file() -> Fetcher {
        let path = env::temp_dir().join(gen_random_filename());

        Fetcher::create(&path, 64, 32)
            .await
            .expect("Failed to create file fetcher")
    }

    #[tokio::test]
    async fn creates_a_file() {
        let path = env::temp_dir().join(gen_random_filename());

        assert_ok!(Fetcher::create(&path, 64, 32).await);

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
        let invalid_paths = vec![Path::new("/"), Path::new("")];

        for invalid_path in invalid_paths {
            let err = assert_err!(Fetcher::create(&invalid_path, 64, 32).await);
            assert_matches!(err, FetcherError::IoError { path, source }
                if path == invalid_path && source.kind() == std::io::ErrorKind::InvalidInput);
        }
    }

    #[tokio::test]
    async fn write_blocks_and_read_chunks() {
        let fetcher = create_random_file().await;

        let block = Block::new(0, vec![1; 32]);
        assert_ok!(fetcher.write_block(block).await);

        let chunk = Chunk::new(0, 0, 32);
        let data = assert_ok!(fetcher.read_chunk(chunk).await);
        assert!(data.iter().all(|&b| b == 1));

        let chunk = Chunk::new(1, 0, 32);
        let data = assert_ok!(fetcher.read_chunk(chunk).await);
        assert!(data.iter().all(|&b| b == 0));

        let block = Block::new(1, vec![2; 32]);
        assert_ok!(fetcher.write_block(block).await);

        let chunk = Chunk::new(1, 0, 32);
        let data = assert_ok!(fetcher.read_chunk(chunk).await);
        assert!(data.iter().all(|&b| b == 2));

        let mut data = vec![3; 16];
        data.append(&mut vec![4; 16]);
        let block = Block::new(0, data);
        assert_ok!(fetcher.write_block(block).await);

        let chunk = Chunk::new(0, 8, 16);
        let data = assert_ok!(fetcher.read_chunk(chunk).await);
        assert!(data[..8].iter().all(|&b| b == 3));
        assert!(data[8..].iter().all(|&b| b == 4));
    }

    #[tokio::test]
    async fn fail_to_read_with_invalid_block_offset() {
        let fetcher = create_random_file().await;

        let invalid_chunks = vec![Chunk::new(u64::MAX, 0, 32), Chunk::new(2, 0, 32)];

        for invalid_chunk in invalid_chunks {
            let err = assert_err!(fetcher.read_chunk(invalid_chunk).await);
            assert_matches!(err, FetcherError::InvalidBlockOffset);
        }
    }

    #[tokio::test]
    async fn fail_to_write_with_invalid_block_offset() {
        let fetcher = create_random_file().await;

        let invalid_blocks = vec![
            Block::new(u64::MAX, vec![1; 32]),
            Block::new(2, vec![1; 32]),
        ];

        for invalid_block in invalid_blocks {
            let err = assert_err!(fetcher.write_block(invalid_block).await);
            assert_matches!(err, FetcherError::InvalidBlockOffset);
        }
    }

    #[tokio::test]
    async fn fail_to_read_with_invalid_chunk_offset() {
        let fetcher = create_random_file().await;

        let invalid_chunks = vec![Chunk::new(1, u64::MAX, 32), Chunk::new(1, 64, 32)];

        for invalid_chunk in invalid_chunks {
            let err = assert_err!(fetcher.read_chunk(invalid_chunk).await);
            assert_matches!(err, FetcherError::InvalidChunkOffset);
        }
    }

    #[tokio::test]
    async fn fail_to_read_when_chunk_exceeds_limits() {
        let fetcher = create_random_file().await;

        let invalid_chunks = vec![Chunk::new(1, 0, u64::MAX), Chunk::new(1, 0, 65)];

        for invalid_chunk in invalid_chunks {
            let err = assert_err!(fetcher.read_chunk(invalid_chunk).await);
            assert_matches!(err, FetcherError::InvalidChunkSize);
        }
    }
}
