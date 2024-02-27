use thiserror::Error;

#[derive(Error, Debug)]
pub enum RbitError {
    #[error("Invalid Torrent file")]
    InvalidFile,
    #[error("Invalid value of field `{0}`")]
    InvalidFieldValue(&'static str),
    #[error("Error trying get peers: {0}")]
    TrackerFailed(#[from] reqwest::Error),
    #[error("Invalid peers data: {0}")]
    InvalidPeers(&'static str),
    #[error("Tracker returned an error: {0}")]
    TrackerErrorResponse(String),
}
