use thiserror::Error;

#[derive(Error, Debug)]
pub enum RbitError {
    #[error("Invalid Torrent file")]
    InvalidFile,
    #[error("Invalid value of field `{0}`")]
    InvalidField(&'static str),
}
