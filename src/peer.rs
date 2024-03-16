// TODO: remove
#![allow(dead_code)]
#![allow(unused_variables)]

use std::{
    ops::Deref,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{tcp, TcpListener, TcpStream},
    sync::{mpsc, watch},
    time::{interval, Interval},
};

use crate::{file::FileBlock, InfoHash, PeerAddr, Torrent};

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
pub struct PeerBitfieldInner {
    total_pieces: u32,
    pieces: RwLock<Vec<u8>>,
}

impl PeerBitfieldInner {
    fn append_overwrite(&self, raw: &[u8]) -> bool {
        if !valid_bitfield(self.total_pieces, raw) {
            return false;
        }

        self.pieces
            .write()
            .unwrap()
            .iter_mut()
            .zip(raw)
            .for_each(|(current_chunk, new_chunk)| {
                *current_chunk |= new_chunk;
            });

        true
    }

    pub fn has(&self, index: u32) -> Option<bool> {
        if index >= self.total_pieces {
            return None;
        }

        let bindex = BitfieldIndex::new(index);
        let chunk = { self.pieces.read().unwrap()[bindex.chunk_index] };
        let piece = (chunk >> bindex.offset) & 1;

        Some(piece == 1)
    }

    pub fn set(&self, index: u32) {
        if index >= self.total_pieces {
            return;
        }

        let bindex = BitfieldIndex::new(index);
        self.pieces.write().unwrap()[bindex.chunk_index] |= 1 << bindex.offset;
    }

    pub fn raw(&self) -> Vec<u8> {
        self.pieces.read().unwrap().clone()
    }
}

#[derive(Debug, Clone)]
pub struct PeerBitfield {
    inner: Arc<PeerBitfieldInner>,
}

impl PeerBitfield {
    pub fn new(total_pieces: u32) -> Self {
        let length = bitfield_chunks(total_pieces);
        let pieces = RwLock::new(vec![0; length as usize]);

        let inner = Arc::new(PeerBitfieldInner {
            total_pieces,
            pieces,
        });

        Self { inner }
    }

    pub fn from_bytes(total_pieces: u32, raw: &[u8]) -> Option<Self> {
        if !valid_bitfield(total_pieces, raw) {
            return None;
        }

        let pieces = RwLock::new(raw.to_vec());
        let inner = Arc::new(PeerBitfieldInner {
            total_pieces,
            pieces,
        });
        let bitfield = PeerBitfield { inner };

        Some(bitfield)
    }
}

impl Deref for PeerBitfield {
    type Target = PeerBitfieldInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Debug)]
pub enum Message {
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
                buff.extend_from_slice(pieces.as_slice());
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

pub enum PieceBlockSender {
    Client,
    Server,
}

pub struct ClientReceivedPiece {
    pub index: u32,
    pub begin: u32,
    pub piece: FileBlock,
    pub peer_addr: PeerAddr,
}

pub struct PieceBlock {
    pub index: u32,
    pub begin: u32,
    pub length: u32,
    pub peer_addr: PeerAddr,
    pub sender: PieceBlockSender,
}

pub struct PieceBlockRequest(PieceBlock);

impl Deref for PieceBlockRequest {
    type Target = PieceBlock;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<PieceBlock> for PieceBlockRequest {
    fn from(value: PieceBlock) -> Self {
        PieceBlockRequest(value)
    }
}

pub struct CanceledPieceBlock(PieceBlock);

impl Deref for CanceledPieceBlock {
    type Target = PieceBlock;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<PieceBlock> for CanceledPieceBlock {
    fn from(value: PieceBlock) -> Self {
        CanceledPieceBlock(value)
    }
}

struct Switch(AtomicBool);

impl Switch {
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

struct StopperActor(watch::Sender<bool>);

impl StopperActor {
    fn stop(&self) {
        let _ = self.0.send(true);
    }
}

#[derive(Clone)]
struct StopperCheck(watch::Receiver<bool>);

impl StopperCheck {
    async fn stopped_wait(&mut self) -> bool {
        self.0.changed().await.map(|_| true).unwrap_or(true)
    }

    fn stopped(&mut self) -> bool {
        self.0.has_changed().unwrap_or(true)
    }
}

fn stopper() -> (StopperActor, StopperCheck) {
    let (sender, receiver) = watch::channel(false);

    let actor = StopperActor(sender);
    let check = StopperCheck(receiver);

    (actor, check)
}

pub struct PeerState {
    am_choking: Switch,
    peer_choking: Switch,
    am_interested: Switch,
    peer_interested: Switch,
    closed: Switch,
    actor: StopperActor,
}

impl PeerState {
    fn new(actor: StopperActor) -> Self {
        Self {
            am_choking: Switch::new(true),
            peer_choking: Switch::new(true),
            am_interested: Switch::new(false),
            peer_interested: Switch::new(false),
            closed: Switch::new(false),
            actor,
        }
    }

    fn am_choking(&self) {
        self.am_choking.set();
    }

    fn am_unchoking(&self) {
        self.am_choking.unset();
    }

    fn peer_choking(&self) {
        self.peer_choking.set();
    }

    fn peer_unchoking(&self) {
        self.peer_choking.unset();
    }

    fn am_interest(&self) {
        self.am_interested.set();
    }

    fn am_uninsterest(&self) {
        self.am_interested.unset();
    }

    fn peer_interest(&self) {
        self.peer_interested.set();
    }

    fn peer_uninsterest(&self) {
        self.peer_interested.unset();
    }

    fn close(&self) {
        if self.closed() {
            return;
        }

        self.closed.set();

        self.actor.stop();
    }

    pub fn peer_choking_us(&self) -> bool {
        self.peer_choking.current()
    }

    pub fn am_interested_in_peer(&self) -> bool {
        self.am_interested.current()
    }

    pub fn closed(&self) -> bool {
        self.closed.current()
    }
}

#[derive(Clone)]
pub struct BaseSenders {
    pub request_pieces: mpsc::Sender<PieceBlockRequest>,
    pub canceled_pieces: mpsc::Sender<CanceledPieceBlock>,
}

#[derive(Clone)]
pub struct ClientSenders {
    base: BaseSenders,
    pub received_piece_blocks: mpsc::Sender<ClientReceivedPiece>,
}

impl ClientSenders {
    pub fn new(
        request_pieces: mpsc::Sender<PieceBlockRequest>,
        canceled_pieces: mpsc::Sender<CanceledPieceBlock>,
        received_piece_blocks: mpsc::Sender<ClientReceivedPiece>,
    ) -> Self {
        let base = BaseSenders {
            request_pieces,
            canceled_pieces,
        };

        Self {
            base,
            received_piece_blocks,
        }
    }
}

impl Deref for ClientSenders {
    type Target = BaseSenders;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

#[derive(Clone)]
pub struct ServerSenders(BaseSenders);

impl ServerSenders {
    pub fn new(
        request_pieces: mpsc::Sender<PieceBlockRequest>,
        canceled_pieces: mpsc::Sender<CanceledPieceBlock>,
    ) -> Self {
        Self(BaseSenders {
            request_pieces,
            canceled_pieces,
        })
    }
}

impl Deref for ServerSenders {
    type Target = BaseSenders;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

enum StreamRead {
    Received(Message),
    Invalid,
    TimedOut,
    Error,
}

struct StreamReader {
    reader: tcp::OwnedReadHalf,
    heartbeat: Interval,
}

impl StreamReader {
    const HEARTBEAT_TIMEOUT: Duration = Duration::from_millis(2000);

    fn new(reader: tcp::OwnedReadHalf) -> Self {
        let heartbeat = interval(Self::HEARTBEAT_TIMEOUT);

        Self { reader, heartbeat }
    }

    async fn next_message(&mut self) -> StreamRead {
        let msg_len = tokio::select! {
            _ = self.heartbeat.tick() => return StreamRead::TimedOut,
            result = self.reader.read_u32() => {
                match result {
                    Ok(msg_len) => msg_len,
                    Err(_) => return StreamRead::Error,
                }
            }
        };

        let mut raw = vec![0; msg_len as usize];

        tokio::select! {
            _ = self.heartbeat.tick() => return StreamRead::TimedOut,
            result = self.reader.read_exact(&mut raw) => {
                if result.is_err() {
                    return StreamRead::Error;
                }
            }
        }

        Message::decode(&raw)
            .map(StreamRead::Received)
            .unwrap_or(StreamRead::Invalid)
    }
}

struct StreamWriter {
    writer: tcp::OwnedWriteHalf,
    buffer: Vec<u8>,
}

impl StreamWriter {
    fn new(writer: tcp::OwnedWriteHalf) -> Self {
        let buffer = vec![];

        Self { writer, buffer }
    }

    fn fill_buffer(&mut self, msg: Message) {
        msg.encode(&mut self.buffer);
    }

    async fn send_buffered(&mut self) -> Result<bool, tokio::io::Error> {
        if !self.buffer.is_empty() {
            return Ok(false);
        }

        self.writer.write_all(&self.buffer).await?;
        self.writer.flush().await?;

        self.buffer.clear();

        Ok(true)
    }
}

fn split_stream(stream: TcpStream) -> (StreamReader, StreamWriter) {
    let (reader, writer) = stream.into_split();

    let stream_reader = StreamReader::new(reader);
    let stream_writer = StreamWriter::new(writer);

    (stream_reader, stream_writer)
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

const QUEUE_CHECK_TIMEOUT: Duration = Duration::from_millis(1000);

fn spawn_sender(
    mut writer: StreamWriter,
    state: Arc<PeerState>,
    mut checker: StopperCheck,
    mut messages: mpsc::Receiver<Message>,
) {
    tokio::spawn(async move {
        let mut queue_check = interval(QUEUE_CHECK_TIMEOUT);

        loop {
            tokio::select! {
                _ = checker.stopped_wait() => return,
                maybe_message = messages.recv() => {
                    let message = match maybe_message {
                        Some(message) => message,
                        None => return,
                    };

                    writer.fill_buffer(message);
                }
                _ = queue_check.tick() => {
                    match writer.send_buffered().await {
                        Ok(true) => continue,
                        Ok(false) => {
                            writer.fill_buffer(Message::KeepAlive);
                            if writer.send_buffered().await.is_err() {
                                return;
                            }
                        }
                        Err(_) => return,
                    }
                }
            };
        }
    });
}

fn spawn_client_receiver(
    peer_addr: PeerAddr,
    mut reader: StreamReader,
    state: Arc<PeerState>,
    bitfield: Arc<PeerBitfield>,
    mut checker: StopperCheck,
    senders: ClientSenders,
) {
    tokio::spawn(async move {
        loop {
            let message = match reader.next_message().await {
                StreamRead::Received(message) => message,
                StreamRead::Invalid => continue,
                StreamRead::TimedOut => {
                    if checker.stopped() {
                        return;
                    }

                    continue;
                }
                StreamRead::Error => {
                    state.close();
                    return;
                }
            };

            match message {
                Message::KeepAlive => continue,
                Message::Choke => state.am_choking(),
                Message::Unchoke => state.am_unchoking(),
                Message::Interested => state.peer_interest(),
                Message::NotIntersted => state.peer_uninsterest(),
                Message::Have { piece } => bitfield.set(piece),
                Message::Bitfield { pieces } => {
                    if bitfield.append_overwrite(&pieces) {
                        state.close();
                        return;
                    }
                }
                Message::Request {
                    index,
                    begin,
                    length,
                } => {
                    let request = PieceBlock {
                        index,
                        begin,
                        length,
                        peer_addr,
                        sender: PieceBlockSender::Client,
                    }
                    .into();

                    let _ = senders.request_pieces.send(request).await;
                }
                Message::Piece {
                    index,
                    begin,
                    block,
                } => {
                    let piece = block.into();

                    let piece_block = ClientReceivedPiece {
                        index,
                        begin,
                        piece,
                        peer_addr,
                    };

                    let _ = senders.received_piece_blocks.send(piece_block).await;
                }
                Message::Cancel {
                    index,
                    begin,
                    length,
                } => {
                    let cancel = PieceBlock {
                        index,
                        begin,
                        length,
                        peer_addr,
                        sender: PieceBlockSender::Client,
                    }
                    .into();

                    let _ = senders.canceled_pieces.send(cancel).await;
                }
            }
        }
    });
}

pub struct PeerClient {
    state: Arc<PeerState>,
    bitfield: Arc<PeerBitfield>,
    messages_sender: mpsc::Sender<Message>,
}

impl PeerClient {
    const BUFFERED_MESSAGES: usize = 40;

    pub async fn start(
        handshake: Arc<Handshake>,
        mut stream: TcpStream,
        torrent: Arc<Torrent>,
        senders: ClientSenders,
    ) -> Result<Option<Self>, std::io::Error> {
        if !accepted_handshake(handshake, &mut stream).await? {
            return Ok(None);
        }

        let peer_addr = stream.peer_addr().unwrap().into();

        let (actor, checker) = stopper();
        let (reader, writer) = split_stream(stream);
        let (messages_sender, messages_receiver) = mpsc::channel(Self::BUFFERED_MESSAGES);

        let state = Arc::new(PeerState::new(actor));
        let bitfield = Arc::new(PeerBitfield::new(torrent.num_pieces() as u32));

        spawn_sender(writer, state.clone(), checker.clone(), messages_receiver);

        spawn_client_receiver(
            peer_addr,
            reader,
            state.clone(),
            bitfield.clone(),
            checker,
            senders,
        );

        let client = PeerClient {
            state,
            bitfield,
            messages_sender,
        };

        Ok(Some(client))
    }

    pub async fn send_message(&self, message: Message) -> bool {
        self.messages_sender.send(message).await.is_ok()
    }

    pub fn has_piece(&self, index: u32) -> Option<bool> {
        self.bitfield.has(index)
    }
}

impl Deref for PeerClient {
    type Target = PeerState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

pub struct PeerServer {
    state: Arc<PeerState>,
    // TODO: hashmap of (PeerAddr, client
    // (communication channels to know where
    // to send requested/canceled pieces????))
}

impl PeerServer {
    pub fn start(
        handshake: Arc<Handshake>,
        listener: TcpListener,
        torrent: Arc<Torrent>,
        senders: ServerSenders,
    ) -> Result<Self, std::io::Error> {
        todo!()
    }
}

impl Deref for PeerServer {
    type Target = PeerState;

    fn deref(&self) -> &Self::Target {
        &self.state
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
        let pieces = bitfield.raw();
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
        let raw = &[0b10010010, 0b10100101];

        let bitfield = assert_some!(PeerBitfield::from_bytes(16, raw));

        let new_raw = &[0b10011110, 0b10110101];
        assert!(bitfield.append_overwrite(new_raw));

        assert_eq!(bitfield.raw(), new_raw);
    }

    #[test]
    fn overwrite_bitfield_from_invalid_raw_slice() {
        let bitfield = PeerBitfield::new(15);

        let raw = &[0b10110110, 0b10110101];

        assert!(!bitfield.append_overwrite(raw));

        let bitfield = PeerBitfield::new(16);

        let raw = &[0b10110110];

        assert!(!bitfield.append_overwrite(raw));
    }
}
