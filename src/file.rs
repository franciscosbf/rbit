use core::ops::Deref;

pub struct FileBlock(pub Vec<u8>);

impl Deref for FileBlock {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&[u8]> for FileBlock {
    fn from(value: &[u8]) -> Self {
        FileBlock(value.into())
    }
}
