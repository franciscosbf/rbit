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
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};

use crate::{error::RbitError, file::FileBlock, HashPiece, InfoHash};

#[derive(Debug)]
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
struct Handshake<'a> {
    protocol: &'a str,
    info_hash: InfoHash,
    peer_id: PeerId,
}

impl<'a> Handshake<'a> {
    fn decode(raw: &'a [u8]) -> Option<Self> {
        if raw.is_empty() {
            return None;
        }

        let pstrlen = raw[0] as usize;

        if raw.len() != pstrlen + 49 {
            return None;
        }

        let raw = &raw[1..];
        let protocol = unsafe { std::str::from_utf8_unchecked(&raw[..pstrlen]) };

        let raw = &raw[pstrlen + 8..];
        let info_hash = InfoHash(raw[..20].try_into().unwrap());

        let peer_id = (&raw[20..]).into();

        let handshake = Self {
            protocol,
            info_hash,
            peer_id,
        };

        Some(handshake)
    }

    fn encode(self, buff: &mut Vec<u8>) {
        buff.push(self.protocol.len() as u8);
        buff.extend_from_slice(self.protocol.as_bytes());
        buff.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);
        buff.extend_from_slice(&self.info_hash[..]);
        buff.extend_from_slice(&self.peer_id);
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
    index: u64,
    hash: HashPiece,
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
    total_pieces: u64,
    pieces: RwLock<Vec<u8>>,
}

impl PeerBitfield {
    fn new(total_pieces: u64) -> Self {
        let bitfield_length = total_pieces + (8 - total_pieces % 8);
        let pieces = RwLock::new(vec![0; bitfield_length as usize]);

        Self {
            total_pieces,
            pieces,
        }
    }

    fn has(&self, index: u64) -> Option<bool> {
        if index >= self.total_pieces {
            return None;
        }

        let chunk_index = index / 8;
        let chunk = { self.pieces.read().unwrap()[chunk_index as usize] };
        let offset = index % 8;
        let piece = (chunk >> offset) & 1;

        Some(piece == 1)
    }

    fn set(&self, index: u64) {
        if index >= self.total_pieces {
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
    fn new(total_pieces: u64) -> Self {
        Self {
            choke: SwitchState::new(true),
            interest: SwitchState::new(false),
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

pub struct PeerClient {
    base: BasePeer,
    requested_pieces: mpsc::Sender<RequestedPiece>,
}

impl PeerClient {
    pub fn start(
        stream: TcpStream,
        total_pieces: u64,
        received_pieces: mpsc::Sender<ReceivedPiece>,
        missing_pieces: mpsc::Sender<MissingPiece>,
    ) -> Result<Self, RbitError> {
        todo!()
    }

    pub async fn request_piece(&self, index: u64, hash: HashPiece) -> bool {
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

pub struct PeerServer(BasePeer);

impl PeerServer {
    pub fn start(
        listener: TcpListener,
        total_pieces: u64,
        request_pieces: mpsc::Sender<PieceBlockRequest>,
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use claims::{assert_matches, assert_none, assert_ok, assert_some};
    use tokio::{
        io::AsyncReadExt,
        net::{TcpListener, TcpStream},
        sync::mpsc,
        time::sleep,
    };
    use tokio_stream::StreamExt;

    use crate::InfoHash;

    use super::{Handshake, Message, PeerClient, PeerId};

    #[test]
    fn encode_handshake() {
        let handshake = Handshake {
            protocol: "BitTorrent protocol",
            info_hash: InfoHash(*b"AAAAAAAAAAAAAAAAAAAA"),
            peer_id: PeerId(*b"-RB0100-TPhjEUZd5yX2"),
        };

        let mut buff = vec![];
        handshake.encode(&mut buff);

        assert_eq!(
            buff,
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

    // #[tokio::test]
    // async fn request_piece_to_and_receive_it() {
    //     let (send_pieces, mut receive_pieces) = mpsc::channel(1);
    //     let (send_missing_pieces, mut receive_missing_pieces) = mpsc::channel(1);
    //
    //     let listener: TcpListener =
    //         TcpListener::bind("localhost:0").expect("Failed to bind address");
    //     let addr = listener
    //         .local_addr()
    //         .expect("Failed to retrieve server address");
    //     let peer = tokio::spawn(async move {
    //         match listener.accept().await {
    //             Ok((stream, _)) => {
    //                 // TODO: send back handshake.
    //
    //                 // TODO: verify received message.
    //                 // let (reader, writer) = stream.into_split();
    //                 // let msg_len = assert_ok!(stream.read_u32().await);
    //                 // let mut raw_msg = [0_u8; msg_len];
    //                 // assert_ok!(stream.read_buf(&mut raw_msg).await);
    //                 // let msg = assert_some!(Message::decode(&raw_msg));
    //                 // match msg {
    //                 //     Message::Request { index, begin, length } => {
    //                 //
    //                 //     }
    //                 // }
    //             }
    //             Err(e) => panic!("Failed to accept connection: {}", e),
    //         }
    //     });
    //
    //     let stream = TcpStream::connect(addr)
    //         .await
    //         .expect("Failed to connect to listener");
    //     let client = assert_ok!(PeerClient::start(
    //         stream,
    //         1,
    //         send_pieces,
    //         send_missing_pieces
    //     ));
    //
    //     tokio::select! {
    //         _ = sleep(Duration::from_millis(1000)) => {
    //             panic!("Didn't receive piece from client");
    //         }
    //         _ = receive_missing_pieces => {
    //             panic!("Received from peer in missing pieces channel");
    //         }
    //         received = received_pieces => {
    //             assert_eq!(received.index, 0);
    //             assert_eq!(received.piece.0, b"\xA2\x23\x45".as_slice());
    //         }
    //     }
    //
    //     assert_ok!(peer.await);
    // }

    // #[tokio::test]
    // async fn receive_piece_request_in_server_and_give_it() {
    //     let (sender, receiver) = mpsc::channel(1);
    // }
}
