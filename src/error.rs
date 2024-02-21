use thiserror::Error;

#[derive(Error, Debug)]
pub enum RbitError {
    #[error("Invalid Torrent file")]
    ParseFailed,
}
