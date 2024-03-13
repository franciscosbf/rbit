// TODO: remove
#![allow(dead_code)]
#![allow(unused_variables)]

use std::{
    net::SocketAddr,
    ops::Deref,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};

use crate::{file::FileBlock, HashPiece, InfoHash, PeerAddr};

#[derive(Debug, Clone, Copy)]
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

impl From<&[u8]> for PeerId {
    fn from(value: &[u8]) -> Self {
        PeerId(value.try_into().unwrap())
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

#[derive(Debug)]
pub struct Handshake {
    protocol: String,
    info_hash: InfoHash,
    peer_id: PeerId,
    encoded: Vec<u8>,
}

impl Handshake {
    pub fn new(info_hash: InfoHash, peer_id: PeerId) -> Self {
        let mut encoded = vec![0];

        let protocol = String::from("BitTorrent protocol");

        encoded[0] = 19;
        encoded.extend_from_slice(protocol.as_bytes());
        encoded.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);
        encoded.extend_from_slice(&info_hash[..]);
        encoded.extend_from_slice(&peer_id);

        Self {
            protocol,
            info_hash,
            peer_id,
            encoded,
        }
    }

    fn decode(raw: &[u8]) -> Option<Self> {
        if raw.is_empty() {
            return None;
        }

        let pstrlen = raw[0] as usize;

        if raw.len() != pstrlen + 49 {
            return None;
        }

        let mut encoded = vec![0];

        encoded[0] = pstrlen as u8;

        let raw = &raw[1..];
        let protocol = unsafe { std::str::from_utf8_unchecked(&raw[..pstrlen]) }.into();

        encoded.extend_from_slice(&raw[..pstrlen]);
        encoded.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);

        let raw = &raw[pstrlen + 8..];
        let info_hash = InfoHash(raw[..20].try_into().unwrap());

        encoded.extend_from_slice(&raw[..20]);

        let peer_id = (&raw[20..]).into();

        encoded.extend_from_slice(&raw[20..]);

        let handshake = Self {
            protocol,
            info_hash,
            peer_id,
            encoded,
        };

        Some(handshake)
    }

    fn raw(&self) -> &[u8] {
        &self.encoded
    }
}

#[derive(Debug)]
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

struct RequestedPiece {
    index: u32,
    hash: HashPiece,
}

pub struct ReceivedPiece {
    pub index: u32,
    pub begin: u32,
    pub piece: FileBlock,
    pub peer_addr: PeerAddr,
}

pub struct PieceBlockRequest {
    pub index: u32,
    pub begin: u32,
    pub length: u32,
    pub peer_addr: PeerAddr,
    available_blocks: mpsc::Sender<FileBlock>,
}

pub struct CanceledPieceBlock {
    pub index: u32,
    pub begin: u32,
    pub length: u32,
    pub peer_addr: PeerAddr,
}

impl PieceBlockRequest {
    pub async fn give(self, block: FileBlock) {
        let _ = self.available_blocks.send(block).await;
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

fn bitfield_chunks(total_pieces: u32) -> u32 {
    (total_pieces as f64 / 8.).ceil() as u32
}

struct BitfieldIndex {
    chunk_index: usize,
    offset: u32,
}

impl BitfieldIndex {
    fn new(index: u32) -> Self {
        let chunk_index = index as usize / 8;
        let offset = 7 - index % 8;

        Self {
            chunk_index,
            offset,
        }
    }
}

fn valid_bitfield(total_pieces: u32, raw: &[u8]) -> bool {
    bitfield_chunks(total_pieces) as usize == raw.len()
        && (total_pieces % 8 == 0 || {
            let last_piece_index = total_pieces - 1;
            let bindex = BitfieldIndex::new(last_piece_index);
            let last_chunk = raw[bindex.chunk_index];

            (0..bindex.offset).all(|offset| (last_chunk >> offset) & 1 == 0)
        })
}

#[derive(Debug)]
struct PeerBitfield {
    total_pieces: u32,
    pieces: RwLock<Vec<u8>>,
}

impl PeerBitfield {
    fn new(total_pieces: u32) -> Self {
        let length = bitfield_chunks(total_pieces);
        let pieces = RwLock::new(vec![0; length as usize]);

        Self {
            total_pieces,
            pieces,
        }
    }

    fn from_bytes(total_pieces: u32, raw: &[u8]) -> Option<Self> {
        if !valid_bitfield(total_pieces, raw) {
            return None;
        }

        let pieces = RwLock::new(raw.to_vec());
        let bitfield = Self {
            total_pieces,
            pieces,
        };

        Some(bitfield)
    }

    fn overwrite(&self, raw: &[u8]) -> bool {
        if !valid_bitfield(self.total_pieces, raw) {
            return false;
        }

        self.pieces.write().unwrap().copy_from_slice(raw);

        true
    }

    fn has(&self, index: u32) -> Option<bool> {
        if index >= self.total_pieces {
            return None;
        }

        let bindex = BitfieldIndex::new(index);
        let chunk = { self.pieces.read().unwrap()[bindex.chunk_index] };
        let piece = (chunk >> bindex.offset) & 1;

        Some(piece == 1)
    }

    fn set(&self, index: u32) {
        if index >= self.total_pieces {
            return;
        }

        let bindex = BitfieldIndex::new(index);
        self.pieces.write().unwrap()[bindex.chunk_index] |= 1 << bindex.offset;
    }

    fn feed(&self, buff: &mut Vec<u8>) {
        buff.extend_from_slice(self.pieces.read().unwrap().as_slice())
    }
}

pub struct PeerState {
    choke: SwitchState,
    interest: SwitchState,
    closed: SwitchState,
    bitfield: PeerBitfield,
}

impl PeerState {
    fn new(total_pieces: u32) -> Self {
        Self {
            choke: SwitchState::new(true),
            interest: SwitchState::new(false),
            closed: SwitchState::new(false),
            bitfield: PeerBitfield::new(total_pieces),
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

    fn set_piece(&self, index: u32) {
        self.bitfield.set(index);
    }

    fn set_pieces(&self, raw: &[u8]) -> bool {
        self.bitfield.overwrite(raw)
    }

    fn set_closed(&self) {
        self.closed.set();
    }

    pub fn choked(&self) -> bool {
        self.choke.current()
    }

    pub fn interested(&self) -> bool {
        self.interest.current()
    }

    pub fn closed(&self) -> bool {
        self.closed.current()
    }

    pub fn has_piece(&self, index: u32) -> Option<bool> {
        self.bitfield.has(index)
    }
}

async fn accepted_handshake(
    handshake: Arc<Handshake>,
    stream: &mut TcpStream,
) -> Result<bool, std::io::Error> {
    let raw = handshake.raw();

    stream.write_all(raw).await?;

    let mut handshake_reply = vec![0; raw.len()];

    stream.read_exact(&mut handshake_reply).await?;

    let match_handshake = raw == handshake_reply;

    Ok(match_handshake)
}

pub struct ChannelSenders {
    pub received_pieces: mpsc::Sender<ReceivedPiece>,
    pub request_pieces: mpsc::Sender<PieceBlockRequest>,
    pub canceled_pieces: mpsc::Sender<CanceledPieceBlock>,
}

async fn spawn_client_msgs_sender(
    peer_addr: PeerAddr,
    is_client: bool,
    mut sender: WriteHalf<TcpStream>,
    state: Arc<PeerState>,
    requested_pieces: mpsc::Sender<RequestedPiece>,
    available_blocks: mpsc::Sender<FileBlock>,
) {
    todo!();
}

async fn spawn_server_msgs_sender(
    peer_addr: PeerAddr,
    is_client: bool,
    mut sender: WriteHalf<TcpStream>,
    state: Arc<PeerState>,
    available_blocks: mpsc::Sender<FileBlock>,
) {
    todo!();
}

async fn spawn_msgs_receiver(
    peer_addr: PeerAddr,
    is_client: bool,
    mut receiver: ReadHalf<TcpStream>,
    state: Arc<PeerState>,
    channel_senders: ChannelSenders,
    sender_available_blocks: mpsc::Sender<FileBlock>,
) {
    tokio::spawn(async move {
        let timeout = tokio::time::sleep(Duration::from_millis(1000));
        tokio::pin!(timeout);

        loop {
            let msg_len = tokio::select! {
                _ = &mut timeout => {
                    if state.closed() {
                        return;
                    }

                    continue;
                },
                result = receiver.read_u32() => {
                    match result {
                        Ok(msg_len) => msg_len,
                        Err(_) => break,
                    }
                }
            };

            let msg_len = match receiver.read_u32().await {
                Ok(msg_len) => msg_len,
                Err(_) => break,
            };

            let mut raw = vec![0_u8; msg_len as usize];

            if receiver.read_exact(&mut raw).await.is_err() {
                break;
            }

            let msg = match Message::decode(&raw) {
                Some(msg) => msg,
                None => continue,
            };

            match msg {
                Message::KeepAlive => (),
                Message::Choke => state.choke(),
                Message::Unchoke => state.unchoke(),
                Message::Interested => state.interest(),
                Message::NotIntersted => state.uninsterest(),
                Message::Have { piece } if is_client => state.set_piece(piece),
                Message::Bitfield { pieces } if !is_client => {
                    if !state.set_pieces(&pieces) {
                        break;
                    }
                }
                Message::Request {
                    index,
                    begin,
                    length,
                } => {
                    let available_blocks = sender_available_blocks.clone();

                    let request = PieceBlockRequest {
                        index,
                        begin,
                        length,
                        peer_addr,
                        available_blocks,
                    };
                }
                Message::Piece {
                    index,
                    begin,
                    block,
                } => {
                    let piece = block.as_slice().into();

                    let received = ReceivedPiece {
                        index,
                        begin,
                        piece,
                        peer_addr,
                    };

                    if channel_senders
                        .received_pieces
                        .send(received)
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Message::Cancel {
                    index,
                    begin,
                    length,
                } => {
                    let canceled = CanceledPieceBlock {
                        index,
                        begin,
                        length,
                        peer_addr,
                    };

                    if channel_senders
                        .canceled_pieces
                        .send(canceled)
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                _ => (),
            }
        }

        state.set_closed();
    });
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

pub struct PeerClient {
    base: BasePeer,
    requested_pieces: mpsc::Sender<RequestedPiece>,
}

impl PeerClient {
    pub async fn start(
        handshake: Arc<Handshake>,
        mut stream: TcpStream,
        total_pieces: u32,
        channel_senders: ChannelSenders,
    ) -> Result<Option<Self>, std::io::Error> {
        if !accepted_handshake(handshake, &mut stream).await? {
            return Ok(None);
        }

        todo!()
    }

    pub async fn request_piece(&self, index: u32, hash: HashPiece) -> bool {
        let request = RequestedPiece { index, hash };

        self.requested_pieces.send(request).await.is_ok()
    }
}

impl Deref for PeerClient {
    type Target = BasePeer;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

pub struct PeerServer {
    base: BasePeer,
}

impl PeerServer {
    pub fn start(
        listener: TcpListener,
        total_pieces: u32,
        channel_senders: ChannelSenders,
    ) -> Result<Self, std::io::Error> {
        todo!()
    }

    pub fn mark_available_piece(&self, index: u32) {
        self.state.set_piece(index);
    }
}

impl Deref for PeerServer {
    type Target = BasePeer;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

#[cfg(test)]
mod tests {
    use claims::{assert_matches, assert_none, assert_some, assert_some_eq};

    use crate::InfoHash;

    use super::{Handshake, Message, PeerBitfield, PeerId};

    #[test]
    fn encode_handshake() {
        let handshake = Handshake::new(
            InfoHash(*b"AAAAAAAAAAAAAAAAAAAA"),
            PeerId(*b"-RB0100-TPhjEUZd5yX2"),
        );

        assert_eq!(
            handshake.raw(),
            b"\x13BitTorrent protocol\
            \x00\x00\x00\x00\x00\x00\x00\x00\
            AAAAAAAAAAAAAAAAAAAA\
            -RB0100-TPhjEUZd5yX2"
        );
    }

    #[test]
    fn decode_valid_handshake() {
        let raw = b"\x13BitTorrent protocol\
            \x00\x00\x00\x00\x00\x00\x00\x00\
            AAAAAAAAAAAAAAAAAAAA\
            -RB0100-TPhjEUZd5yX2";

        let handshake = assert_some!(Handshake::decode(raw));
        assert_eq!(handshake.protocol, "BitTorrent protocol");
        assert_eq!(&handshake.info_hash.0, b"AAAAAAAAAAAAAAAAAAAA");
        assert_eq!(&handshake.peer_id.0, b"-RB0100-TPhjEUZd5yX2");
        assert_eq!(handshake.raw(), raw);
    }

    #[test]
    fn decode_empty_handshake() {
        let raw = b"";

        assert_none!(Handshake::decode(raw));
    }

    #[test]
    fn decode_invalid_handshake_size() {
        let raw = b"\x13BitTorrent protocol\
            -RB0100-TPhjEUZd5yX2";

        assert_none!(Handshake::decode(raw));
    }

    #[test]
    fn encode_keep_alive_message() {
        let mut buff = vec![];
        Message::KeepAlive.encode(&mut buff);

        assert_eq!(buff, *b"\x00\x00\x00\x00");
    }

    #[test]
    fn decode_keep_alive_message() {
        let msg = assert_some!(Message::decode(b""));
        assert_matches!(msg, Message::KeepAlive);
    }

    #[test]
    fn encode_choke_message() {
        let mut buff = vec![];
        Message::Choke.encode(&mut buff);

        assert_eq!(buff, *b"\x00\x00\x00\x01\x00");
    }

    #[test]
    fn decode_choke_message() {
        let msg = assert_some!(Message::decode(b"\x00"));
        assert_matches!(msg, Message::Choke);
    }

    #[test]
    fn encode_unchoke_message() {
        let mut buff = vec![];
        Message::Unchoke.encode(&mut buff);

        assert_eq!(buff, *b"\x00\x00\x00\x01\x01");
    }

    #[test]
    fn decode_unchoke_message() {
        let msg = assert_some!(Message::decode(b"\x01"));
        assert_matches!(msg, Message::Unchoke);
    }

    #[test]
    fn encode_interested_message() {
        let mut buff = vec![];
        Message::Interested.encode(&mut buff);

        assert_eq!(buff, *b"\x00\x00\x00\x01\x02");
    }

    #[test]
    fn decode_interested_message() {
        let msg = assert_some!(Message::decode(b"\x02"));
        assert_matches!(msg, Message::Interested);
    }

    #[test]
    fn encode_not_interested_message() {
        let mut buff = vec![];
        Message::NotIntersted.encode(&mut buff);

        assert_eq!(buff, *b"\x00\x00\x00\x01\x03");
    }

    #[test]
    fn decode_not_interested_message() {
        let msg = assert_some!(Message::decode(b"\x03"));
        assert_matches!(msg, Message::NotIntersted);
    }

    #[test]
    fn encode_have_message() {
        let mut buff = vec![];
        Message::Have { piece: 1254 }.encode(&mut buff);

        assert_eq!(buff, *b"\x00\x00\x00\x05\x04\x00\x00\x04\xe6");
    }

    #[test]
    fn decode_have_message() {
        let msg = assert_some!(Message::decode(b"\x04\x00\x00\x04\xe6"));
        assert_matches!(msg, Message::Have { piece: 1254 });
    }

    #[test]
    fn encode_bitfield_message() {
        let mut buff = vec![];
        Message::Bitfield {
            pieces: b"\xd9\x0c\x73\x24\x7c\xcb\xfc\xb6\x39\x95".to_vec(),
        }
        .encode(&mut buff);

        assert_eq!(
            buff,
            *b"\x00\x00\x00\x0b\x05\xd9\x0c\x73\x24\x7c\xcb\xfc\xb6\x39\x95"
        );
    }

    #[test]
    fn decode_bitfield_message() {
        let msg = assert_some!(Message::decode(
            b"\x05\xd9\x0c\x73\x24\x7c\xcb\xfc\xb6\x39\x95"
        ));
        let pieces = b"\xd9\x0c\x73\x24\x7c\xcb\xfc\xb6\x39\x95".to_vec();
        assert_matches!(msg, Message::Bitfield { pieces });
    }

    #[test]
    fn encode_request_message() {
        let mut buff = vec![];
        Message::Request {
            index: 43,
            begin: 23,
            length: 556,
        }
        .encode(&mut buff);

        assert_eq!(
            buff,
            *b"\x00\x00\x00\x0d\x06\x00\x00\x00\x2b\x00\x00\x00\x17\x00\x00\x02\x2c"
        );
    }

    #[test]
    fn decode_request_message() {
        let msg = assert_some!(Message::decode(
            b"\x06\x00\x00\x00\x2b\x00\x00\x00\x17\x00\x00\x02\x2c"
        ));
        assert_matches!(
            msg,
            Message::Request {
                index: 43,
                begin: 23,
                length: 556
            }
        );
    }

    #[test]
    fn encode_piece_message() {
        let mut buff = vec![];
        Message::Piece {
            index: 43,
            begin: 23,
            block: b"\xd9\x0c\x73\x24\x7c\xcb\xfc\xb6\x39\x95".to_vec(),
        }
        .encode(&mut buff);

        assert_eq!(
            buff,
            *b"\x00\x00\x00\x13\x07\x00\x00\x00\x2b\x00\x00\x00\x17\
                \xd9\x0c\x73\x24\x7c\xcb\xfc\xb6\x39\x95"
        );
    }

    #[test]
    fn decode_piece_message() {
        let msg = assert_some!(Message::decode(
            b"\x07\x00\x00\x00\x2b\x00\x00\x00\x17\
                \xd9\x0c\x73\x24\x7c\xcb\xfc\xb6\x39\x95"
        ));
        let block = b"\xd9\x0c\x73\x24\x7c\xcb\xfc\xb6\x39\x95".to_vec();
        assert_matches!(
            msg,
            Message::Piece {
                index: 43,
                begin: 23,
                block,
            }
        );
    }

    #[test]
    fn encode_cancel_message() {
        let mut buff = vec![];
        Message::Cancel {
            index: 43,
            begin: 23,
            length: 556,
        }
        .encode(&mut buff);

        assert_eq!(
            buff,
            *b"\x00\x00\x00\x0d\x08\x00\x00\x00\x2b\x00\x00\x00\x17\x00\x00\x02\x2c"
        );
    }

    #[test]
    fn decode_cancel_message() {
        let msg = assert_some!(Message::decode(
            b"\x08\x00\x00\x00\x2b\x00\x00\x00\x17\x00\x00\x02\x2c"
        ));
        assert_matches!(
            msg,
            Message::Cancel {
                index: 43,
                begin: 23,
                length: 556
            }
        );
    }

    #[test]
    fn decode_invalid_message() {
        assert_none!(Message::decode(b"\x69\x69"));
    }

    #[test]
    fn create_empty_peer_bitfield() {
        let bitfield = PeerBitfield::new(42);

        assert_eq!(bitfield.total_pieces, 42);
        let pieces = bitfield.pieces.read().unwrap();
        assert_eq!(pieces.len(), 6);
        assert!(pieces.iter().all(|&chunk| chunk == 0));
    }

    #[test]
    fn create_peer_bitfield_from_valid_raw_slice() {
        let raw = &[0b10110110, 0b10110111, 0b01010011, 0b10110000];

        let bitfield = assert_some!(PeerBitfield::from_bytes(30, raw));

        assert_eq!(bitfield.total_pieces, 30);
        let pieces = bitfield.pieces.read().unwrap();
        assert_eq!(pieces.len(), 4);
        assert_eq!(pieces.as_slice(), raw);
    }

    #[test]
    fn create_peer_bitfield_from_valid_raw_slice_with_number_of_pieces_multiple_of_8() {
        let raw = &[0b10110110, 0b10110111, 0b01010011];

        assert_some!(PeerBitfield::from_bytes(24, raw));
    }

    #[test]
    fn create_peer_bitfield_from_invalid_raw_slice() {
        let raw = &[0b10110110, 0b10110111, 0b01010011, 0b10110000];

        assert_none!(PeerBitfield::from_bytes(27, raw));
    }

    #[test]
    fn create_peer_bitfield_from_invalid_pieces_length_with_raw_slice() {
        let raw = &[0b10110110, 0b10110111, 0b01010011, 0b10110000];

        assert_none!(PeerBitfield::from_bytes(16, raw));
    }

    #[test]
    fn check_pieces_set_in_peer_bitfield() {
        let raw = &[0b10010010, 0b10100101];

        let bitfield = assert_some!(PeerBitfield::from_bytes(16, raw));

        assert_none!(bitfield.has(16));
        assert_none!(bitfield.has(69));

        [0, 3, 6, 8, 10, 13, 15].iter().for_each(|&index| {
            assert_some_eq!(
                bitfield.has(index),
                true,
                "failed with pice of index {}",
                index
            );
        });
    }

    #[test]
    fn mark_pieces_in_bitfield() {
        let bitfield = PeerBitfield::new(24);

        bitfield.set(24);
        bitfield.set(65);

        assert_eq!(bitfield.pieces.read().unwrap().as_slice(), &[0, 0, 0]);

        [0, 3, 6, 8, 10, 13, 15, 16, 18, 23]
            .iter()
            .for_each(|&index| {
                bitfield.set(index);

                assert!(
                    bitfield.has(index).unwrap(),
                    "pice of index {} isn't set",
                    index
                );
            });
    }

    #[test]
    fn overwrite_bitfield() {
        let bitfield = PeerBitfield::new(16);

        let raw = &[0xFF, 0xFF];

        assert!(bitfield.overwrite(raw));

        assert_eq!(bitfield.pieces.read().unwrap().as_slice(), raw);
    }

    #[test]
    fn overwrite_bitfield_from_invalid_raw_slice() {
        let bitfield = PeerBitfield::new(15);

        let raw = &[0b10110110, 0b10110101];

        assert!(!bitfield.overwrite(raw));

        let bitfield = PeerBitfield::new(16);

        let raw = &[0b10110110];

        assert!(!bitfield.overwrite(raw));
    }
}
