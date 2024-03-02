// TODO: remove
#![allow(dead_code)]
#![allow(unused_variables)]

use std::{
    ops::Deref,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
};

use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
};

use crate::{error::RbitError, file::FileBlock, HashPiece, PeerAddr};

pub struct PeerId(pub [u8; 20]);

impl PeerId {
    pub fn build() -> Self {
        let mut identifier = *b"-RB0100-            ";

        let arbitrary = rand::random::<[u8; 12]>();
        identifier[8..].clone_from_slice(&arbitrary);

        Self(identifier)
    }

    pub fn id(&self) -> &[u8] {
        &self.0
    }
}

impl Deref for PeerId {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn bytes_to_u32(raw: &[u8]) -> u32 {
    u32::from_be_bytes(raw.try_into().unwrap())
}

fn u32_to_bytes(value: u32) -> [u8; 4] {
    value.to_be_bytes()
}

enum Message {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotIntersted,
    Have {
        piece: u32,
    },
    Bitfield {
        pieces: Vec<u8>,
    },
    Request {
        index: u32,
        begin: u32,
        length: u32,
    },
    Piece {
        index: u32,
        begin: u32,
        block: Vec<u8>,
    },
    Cancel {
        index: u32,
        begin: u32,
        length: u32,
    },
}

impl Message {
    fn encode(self, buff: &mut Vec<u8>) {
        match self {
            Message::KeepAlive => buff.extend_from_slice(&[0, 0, 0, 0]),
            Message::Choke => buff.extend_from_slice(&[0, 0, 0, 1, 0]),
            Message::Unchoke => buff.extend_from_slice(&[0, 0, 0, 1, 1]),
            Message::Interested => buff.extend_from_slice(&[0, 0, 0, 1, 2]),
            Message::NotIntersted => buff.extend_from_slice(&[0, 0, 0, 1, 3]),
            Message::Have { piece } => {
                buff.extend_from_slice(&[0, 0, 0, 5, 4]);
                buff.extend_from_slice(&u32_to_bytes(piece));
            }
            Message::Bitfield { pieces } => {
                buff.extend_from_slice(&u32_to_bytes(1 + pieces.len() as u32));
                buff.push(5);
                buff.extend_from_slice(&pieces);
            }
            Message::Request {
                index,
                begin,
                length,
            } => {
                buff.extend_from_slice(&[0, 0, 0, 13, 6]);
                buff.extend_from_slice(&u32_to_bytes(index));
                buff.extend_from_slice(&u32_to_bytes(begin));
                buff.extend_from_slice(&u32_to_bytes(length));
            }
            Message::Piece {
                index,
                begin,
                block,
            } => {
                buff.extend_from_slice(&u32_to_bytes(9 + block.len() as u32));
                buff.push(7);
                buff.extend_from_slice(&u32_to_bytes(index));
                buff.extend_from_slice(&u32_to_bytes(begin));
                buff.extend_from_slice(&block);
            }
            Message::Cancel {
                index,
                begin,
                length,
            } => {
                buff.extend_from_slice(&[0, 0, 0, 13, 8]);
                buff.extend_from_slice(&u32_to_bytes(index));
                buff.extend_from_slice(&u32_to_bytes(begin));
                buff.extend_from_slice(&u32_to_bytes(length));
            }
        }
    }

    fn decode(raw: &[u8]) -> Option<Self> {
        if raw.is_empty() {
            return Some(Message::KeepAlive);
        }

        let id = raw[0];
        let len = raw.len();
        let content = &raw[1..];

        match (id, len) {
            (0, 1) => Message::Choke,
            (1, 1) => Message::Unchoke,
            (2, 1) => Message::Interested,
            (3, 1) => Message::NotIntersted,
            (4, 5) => {
                let piece = bytes_to_u32(content);

                Message::Have { piece }
            }
            (5, len) if len > 1 => {
                let pieces = content.into();

                Message::Bitfield { pieces }
            }
            (6, 13) => {
                let index = bytes_to_u32(&content[0..4]);
                let begin = bytes_to_u32(&content[4..8]);
                let length = bytes_to_u32(&content[8..12]);

                Message::Request {
                    index,
                    begin,
                    length,
                }
            }
            (7, len) if len > 9 => {
                let index = bytes_to_u32(&content[0..4]);
                let begin = bytes_to_u32(&content[4..8]);
                let block = content[8..].into();

                Message::Piece {
                    index,
                    begin,
                    block,
                }
            }
            (8, 13) => {
                let index = bytes_to_u32(&content[0..4]);
                let begin = bytes_to_u32(&content[4..8]);
                let length = bytes_to_u32(&content[8..12]);

                Message::Cancel {
                    index,
                    begin,
                    length,
                }
            }
            _ => return None,
        }
        .into()
    }
}

pub struct RequestedPiece {
    pub index: u64,
    pub hash: HashPiece,
}

pub struct ReceivedPiece {
    pub index: u64,
    pub piece: FileBlock,
}

pub struct MissingPiece {
    pub index: u64,
}

pub struct PieceBlockRequest {
    pub index: u64,
    pub begin: u64,
    pub length: u64,
    receive_block: oneshot::Sender<Option<FileBlock>>,
}

impl PieceBlockRequest {
    fn send(self, possible_block: Option<FileBlock>) {
        let _ = self.receive_block.send(possible_block);
    }

    pub fn discard(self) {
        self.send(None)
    }

    pub fn give(self, block: FileBlock) {
        self.send(block.into())
    }
}

struct SwitchState(AtomicBool);

impl SwitchState {
    fn new(initial_state: bool) -> Self {
        Self(AtomicBool::new(initial_state))
    }

    fn set(&self) {
        self.0.store(true, Ordering::Release);
    }

    fn unset(&self) {
        self.0.store(false, Ordering::Release);
    }

    fn current(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

struct PeerBitfield {
    num_pieces: u64,
    pieces: RwLock<Vec<u8>>,
}

impl PeerBitfield {
    fn new(num_pieces: u64) -> Self {
        let bitfield_length = num_pieces + (8 - num_pieces % 8);
        let pieces = RwLock::new(vec![0; bitfield_length as usize]);

        Self { num_pieces, pieces }
    }

    fn has(&self, index: u64) -> Option<bool> {
        if index >= self.num_pieces {
            return None;
        }

        let chunk_index = index / 8;
        let chunk = { self.pieces.read().unwrap()[chunk_index as usize] };
        let offset = index % 8;
        let piece = (chunk >> offset) & 1;

        Some(piece == 1)
    }

    fn set(&self, index: u64) {
        if index >= self.num_pieces {
            return;
        }

        let chunk_index = index / 8;
        let offset = index % 8;
        self.pieces.write().unwrap()[chunk_index as usize] |= 1 << offset;
    }
}

pub struct PeerState {
    choke: SwitchState,
    interest: SwitchState,
    bitfield: PeerBitfield,
}

impl PeerState {
    fn new(num_pieces: u64) -> Self {
        Self {
            choke: SwitchState::new(true),
            interest: SwitchState::new(false),
            bitfield: PeerBitfield::new(num_pieces),
        }
    }

    fn choke(&self) {
        self.choke.set();
    }

    fn unchoke(&self) {
        self.choke.unset();
    }

    fn interest(&self) {
        self.interest.set();
    }

    fn uninsterest(&self) {
        self.interest.unset();
    }

    fn set_piece(&self, index: u64) {
        self.bitfield.set(index);
    }

    pub fn choked(&self) -> bool {
        self.choke.current()
    }

    pub fn interested(&self) -> bool {
        self.interest.current()
    }

    pub fn has_piece(&self, index: u64) -> Option<bool> {
        self.bitfield.has(index)
    }
}

pub struct BasePeer {
    state: Arc<PeerState>,
    stop: oneshot::Sender<()>,
}

impl BasePeer {
    pub fn stop(self) {
        let _ = self.stop.send(());
    }
}

impl Deref for BasePeer {
    type Target = Arc<PeerState>;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

pub struct PeerClient(BasePeer);

impl PeerClient {
    pub fn start(
        address: PeerAddr,
        num_pieces: u64,
        requested_pieces: mpsc::Receiver<RequestedPiece>,
        received_pieces: Arc<mpsc::Sender<ReceivedPiece>>,
        missing_pieces: Arc<mpsc::Sender<MissingPiece>>,
    ) -> Result<Self, RbitError> {
        todo!()
    }
}

impl Deref for PeerClient {
    type Target = BasePeer;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct PeerServer(BasePeer);

impl PeerServer {
    pub fn start(
        stream: TcpListener,
        num_pieces: u64,
        request_pieces: Arc<mpsc::Sender<PieceBlockRequest>>,
    ) -> Result<Self, RbitError> {
        todo!()
    }

    pub fn set_piece(&self, index: u64) {
        self.state.set_piece(index);
    }
}

impl Deref for PeerServer {
    type Target = BasePeer;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
