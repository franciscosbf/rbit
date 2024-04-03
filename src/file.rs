use core::ops::Deref;

#[derive(Debug)]
pub struct FileBlock(pub Vec<u8>);

impl Deref for FileBlock {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Vec<u8>> for FileBlock {
    fn from(value: Vec<u8>) -> Self {
        FileBlock(value)
    }
}
