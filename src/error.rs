use thiserror::Error;

use crate::{PeerError, TorrentError, TrackerError};

#[derive(Error, Debug)]
pub enum RbitError {
    #[error(transparent)]
    Tracker(#[from] TrackerError),
    #[error(transparent)]
    Torrent(#[from] TorrentError),
    #[error(transparent)]
    Peer(#[from] PeerError),
}
