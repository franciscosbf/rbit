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
    time::timeout,
};

use crate::{file::FileBlock, InfoHash, PeerAddr, Torrent};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, PartialEq, Eq)]
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

#[derive(Debug)]
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
    num_chunks: u32,
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

    fn num_chunks(&self) -> u32 {
        self.num_chunks
    }
}

#[derive(Debug, Clone)]
pub struct PeerBitfield {
    inner: Arc<PeerBitfieldInner>,
}

impl PeerBitfield {
    pub fn new(total_pieces: u32) -> Self {
        let num_chunks = bitfield_chunks(total_pieces);
        let pieces = RwLock::new(vec![0; num_chunks as usize]);

        let inner = Arc::new(PeerBitfieldInner {
            total_pieces,
            pieces,
            num_chunks,
        });

        Self { inner }
    }

    pub fn from_bytes(total_pieces: u32, raw: &[u8]) -> Option<Self> {
        if !valid_bitfield(total_pieces, raw) {
            return None;
        }

        let pieces = RwLock::new(raw.to_vec());
        let num_chunks = bitfield_chunks(total_pieces);
        let inner = Arc::new(PeerBitfieldInner {
            total_pieces,
            pieces,
            num_chunks,
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
    pub const MAX_PIECE_CHUNK_SZ: u32 = u32::pow(2, 14);

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

#[derive(Debug, Clone, Copy)]
pub enum PieceBlockSender {
    Client,
    Server,
}

#[derive(Debug)]
pub struct ReceivedPieceBlock {
    pub index: u32,
    pub begin: u32,
    pub piece: FileBlock,
    pub peer_addr: PeerAddr,
}

#[derive(Debug)]
pub struct PieceBlock {
    pub index: u32,
    pub begin: u32,
    pub length: u32,
    pub peer_addr: PeerAddr,
    pub sender: PieceBlockSender,
}

#[derive(Debug)]
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

#[derive(Debug)]
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
    async fn stopped(&mut self) {
        let _ = self.0.changed().await;
    }
}

fn stopper() -> (StopperActor, StopperCheck) {
    let (sender, receiver) = watch::channel(false);

    let actor = StopperActor(sender);
    let check = StopperCheck(receiver);

    (actor, check)
}

pub struct PeerStateInner {
    am_choking: Switch,
    peer_interested: Switch,
    closed: Switch,
    actor: StopperActor,
}

impl PeerStateInner {
    fn new(actor: StopperActor) -> Self {
        Self {
            am_choking: Switch::new(true),
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

    pub fn am_choking_peer(&self) -> bool {
        self.am_choking.current()
    }

    pub fn peer_interested(&self) -> bool {
        self.peer_interested.current()
    }

    pub fn closed(&self) -> bool {
        self.closed.current()
    }
}

#[derive(Clone)]
pub struct PeerState {
    inner: Arc<PeerStateInner>,
}

impl PeerState {
    fn new(actor: StopperActor) -> Self {
        let inner = Arc::new(PeerStateInner::new(actor));

        Self { inner }
    }
}

impl Deref for PeerState {
    type Target = PeerStateInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[trait_variant::make(Send + Sync)]
pub trait Events {
    async fn requested_piece(&self, _piece_block: PieceBlockRequest) {
        async {}
    }
    async fn received_piece_block(&self, _piece_block: ReceivedPieceBlock) {
        async {}
    }
    async fn canceled_piece(&self, _piece_block: CanceledPieceBlock) {
        async {}
    }
}

#[derive(Debug)]
enum StreamRead {
    Received(Message),
    Invalid,
    Error,
    NotReceived,
}

struct StreamReader {
    reader: tcp::OwnedReadHalf,
    buff_max_size: u32,
}

impl StreamReader {
    fn new(reader: tcp::OwnedReadHalf, buff_max_size: u32) -> Self {
        Self {
            reader,
            buff_max_size,
        }
    }

    async fn next_message(&mut self) -> StreamRead {
        let msg_len = match self.reader.read_u32().await {
            Ok(msg_len) if msg_len <= self.buff_max_size => msg_len,
            Ok(_) => return StreamRead::Error,
            Err(_) => return StreamRead::NotReceived,
        };

        let mut raw = vec![0; msg_len as usize];

        if self.reader.read_exact(&mut raw).await.is_err() {
            return StreamRead::NotReceived;
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

    async fn send(&mut self, msg: Message) -> tokio::io::Result<()> {
        msg.encode(&mut self.buffer);
        let result = self.writer.write_all(&self.buffer).await;
        self.buffer.clear();

        result
    }
}

fn split_stream(stream: TcpStream, reader_buff_max_size: u32) -> (StreamReader, StreamWriter) {
    let (reader, writer) = stream.into_split();

    let stream_reader = StreamReader::new(reader, reader_buff_max_size);
    let stream_writer = StreamWriter::new(writer);

    (stream_reader, stream_writer)
}

async fn accepted_handshake(handshake: Arc<Handshake>, stream: &mut TcpStream) -> bool {
    let raw = handshake.raw();

    if stream.write_all(raw).await.is_err() {
        return false;
    }

    let mut handshake_reply = vec![0; raw.len()];

    if stream.read_exact(&mut handshake_reply).await.is_err() {
        println!(">>>>>>hi");
        return false;
    }

    true
}

fn spawn_sender(
    mut writer: StreamWriter,
    state: PeerState,
    mut checker: StopperCheck,
    mut messages: mpsc::Receiver<Message>,
    queue_check_timeout: Duration,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = checker.stopped() => return,
                result = timeout(queue_check_timeout, messages.recv()) => {

                    let message = match result {
                        Ok(Some(message)) => message,
                        Err(_timeout) => Message::KeepAlive,
                        Ok(None) => break,
                    };

                    if writer.send(message).await.is_err() {
                        break;
                    }
                }
            }
        }

        state.close();
    });
}

fn spawn_client_receiver(
    peer_addr: PeerAddr,
    mut reader: StreamReader,
    state: PeerState,
    bitfield: PeerBitfield,
    mut checker: StopperCheck,
    events: Arc<impl Events + 'static>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let read = tokio::select! {
                _ = checker.stopped() => return,
                read = reader.next_message() => read,
            };

            let message = match read {
                StreamRead::Received(message) => message,
                StreamRead::Invalid => continue,
                StreamRead::Error | StreamRead::NotReceived => {
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
                    if !bitfield.append_overwrite(&pieces) {
                        state.close();
                        return;
                    }
                }
                Message::Request {
                    index,
                    begin,
                    length,
                } => {
                    let piece_block = PieceBlock {
                        index,
                        begin,
                        length,
                        peer_addr,
                        sender: PieceBlockSender::Client,
                    }
                    .into();

                    let cevents = events.clone();
                    tokio::spawn(async move {
                        cevents.requested_piece(piece_block).await;
                    });
                }
                Message::Piece {
                    index,
                    begin,
                    block,
                } => {
                    let piece = block.into();

                    let piece_block = ReceivedPieceBlock {
                        index,
                        begin,
                        piece,
                        peer_addr,
                    };

                    let cevents = events.clone();
                    tokio::spawn(async move {
                        cevents.received_piece_block(piece_block).await;
                    });
                }
                Message::Cancel {
                    index,
                    begin,
                    length,
                } => {
                    let piece_block = PieceBlock {
                        index,
                        begin,
                        length,
                        peer_addr,
                        sender: PieceBlockSender::Client,
                    }
                    .into();

                    let cevents = events.clone();
                    tokio::spawn(async move {
                        cevents.canceled_piece(piece_block).await;
                    });
                }
            }
        }
    })
}

fn calc_reader_buff_max_size(piece_size: u32, bitfield_chunks: u32) -> u32 {
    Message::MAX_PIECE_CHUNK_SZ
        .max(piece_size)
        .max(bitfield_chunks)
}

pub struct PeerClient {
    state: PeerState,
    bitfield: PeerBitfield,
    messages_sender: mpsc::Sender<Message>,
}

impl PeerClient {
    const BUFFERED_MESSAGES: usize = 40;
    const QUEUE_CHECK_TIMEOUT: Duration = Duration::from_millis(4000);

    pub async fn start(
        handshake: Arc<Handshake>,
        mut stream: TcpStream,
        torrent: Arc<Torrent>,
        senders: Arc<impl Events + 'static>,
    ) -> Option<Self> {
        if !accepted_handshake(handshake, &mut stream).await {
            return None;
        }

        let bitfield = PeerBitfield::new(torrent.num_pieces() as u32);

        let peer_addr = stream.peer_addr().unwrap().into();

        let reader_buff_max_size =
            calc_reader_buff_max_size(torrent.piece_size as u32, bitfield.num_chunks());

        let (actor, checker) = stopper();
        let state = PeerState::new(actor);

        let (reader, writer) = split_stream(stream, reader_buff_max_size);
        let (messages_sender, messages_receiver) = mpsc::channel(Self::BUFFERED_MESSAGES);

        spawn_sender(
            writer,
            state.clone(),
            checker.clone(),
            messages_receiver,
            Self::QUEUE_CHECK_TIMEOUT,
        );

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

        Some(client)
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
    state: Arc<PeerStateInner>,
    // TODO: hashmap of (PeerAddr, client
    // (communication channels to know where
    // to send requested/canceled pieces????))
}

impl PeerServer {
    pub fn start(
        handshake: Arc<Handshake>,
        listener: TcpListener,
        torrent: Arc<Torrent>,
        senders: Box<impl Events + 'static>,
    ) -> Result<Self, std::io::Error> {
        todo!()
    }
}

impl Deref for PeerServer {
    type Target = PeerStateInner;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc, time::Duration};

    use claims::{assert_matches, assert_none, assert_ok, assert_some, assert_some_eq};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        sync::mpsc,
    };

    use futures::future::{BoxFuture, FutureExt};
    use url::Url;

    use crate::{FileType, InfoHash, PeerAddr, Torrent};

    use super::{
        accepted_handshake, bitfield_chunks, spawn_client_receiver, spawn_sender, stopper,
        BitfieldIndex, CanceledPieceBlock, Events, Handshake, Message, PeerBitfield, PeerClient,
        PeerId, PeerState, PieceBlockRequest, PieceBlockSender, ReceivedPieceBlock, StopperActor,
        StopperCheck, StreamRead, StreamReader, StreamWriter, Switch,
    };

    struct LocalListenerInner {
        listener: tokio::net::TcpListener,
        port: u16,
    }

    impl LocalListenerInner {
        const WAIT_FOR_CONNECTION_LIMIT: Duration = Duration::from_secs(4);

        async fn accept(&self) -> tokio::net::TcpStream {
            let (stream, _) =
                tokio::time::timeout(Self::WAIT_FOR_CONNECTION_LIMIT, self.listener.accept())
                    .await
                    .unwrap()
                    .unwrap();

            stream
        }

        async fn self_connect(&self) -> tokio::net::TcpStream {
            tokio::net::TcpStream::connect(format!("localhost:{}", self.port))
                .await
                .unwrap()
        }

        fn addr(&self) -> SocketAddr {
            self.listener.local_addr().unwrap()
        }
    }

    #[derive(Clone)]
    struct LocalListener {
        inner: Arc<LocalListenerInner>,
    }

    impl std::ops::Deref for LocalListener {
        type Target = LocalListenerInner;

        fn deref(&self) -> &Self::Target {
            &self.inner
        }
    }

    impl LocalListener {
        async fn build() -> Self {
            let listener = tokio::net::TcpListener::bind("localhost:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();

            let inner = Arc::new(LocalListenerInner { listener, port });

            Self { inner }
        }
    }

    async fn validate_stream_reader<CF, PF>(cli_action: CF, peer_action: PF)
    where
        CF: FnOnce(StreamReader) -> BoxFuture<'static, ()> + Send + 'static,
        PF: FnOnce(tokio::net::tcp::OwnedWriteHalf) -> BoxFuture<'static, ()> + Send + 'static,
    {
        let listener = LocalListener::build().await;
        let clistener = listener.clone();

        let tcli = tokio::spawn(async move {
            let stream = clistener.self_connect().await;
            let (reader, _) = stream.into_split();
            let sreader = StreamReader::new(reader, Message::MAX_PIECE_CHUNK_SZ);

            cli_action(sreader).await;
        });

        let tpeer = tokio::spawn(async move {
            let stream = listener.accept().await;
            let (_, writer) = stream.into_split();

            peer_action(writer).await;
        });

        assert_ok!(tcli.await);

        assert_ok!(tpeer.await);
    }

    async fn validate_stream_writer<SF, PF>(server_action: SF, peer_action: PF)
    where
        SF: FnOnce(StreamWriter) -> BoxFuture<'static, ()> + Send + 'static,
        PF: FnOnce(tokio::net::tcp::OwnedReadHalf) -> BoxFuture<'static, ()> + Send + 'static,
    {
        let listener = LocalListener::build().await;
        let clistener = listener.clone();

        let tpeer = tokio::spawn(async move {
            let stream = clistener.accept().await;
            let (reader, _) = stream.into_split();

            peer_action(reader).await;
        });

        let tsrv = tokio::spawn(async move {
            let stream = listener.self_connect().await;
            let (_, writer) = stream.into_split();
            let stream_writer = StreamWriter::new(writer);

            server_action(stream_writer).await;
        });

        assert_ok!(tpeer.await);

        assert_ok!(tsrv.await);
    }

    async fn validate_handshake<PF>(peer_reply: PF) -> bool
    where
        PF: FnOnce(
                Arc<Handshake>,
                tokio::net::tcp::OwnedReadHalf,
                tokio::net::tcp::OwnedWriteHalf,
            ) -> BoxFuture<'static, ()>
            + Send
            + 'static,
    {
        let listener = LocalListener::build().await;
        let clistener = listener.clone();

        let info_hash = InfoHash::hash(&[1, 2, 3, 4]);
        let peer_id = PeerId::build();
        let handshake = Arc::new(Handshake::new(info_hash, peer_id));
        let chandshake = Arc::clone(&handshake);

        let tcli = tokio::spawn(async move {
            let mut stream = clistener.accept().await;

            accepted_handshake(chandshake, &mut stream).await
        });

        let tpeer = tokio::spawn(async move {
            let stream = listener.self_connect().await;
            let (reader, writer) = stream.into_split();

            peer_reply(handshake, reader, writer).await;
        });

        assert_ok!(tpeer.await);

        assert_ok!(tcli.await)
    }

    async fn validate_spawn_sender<CF, PF>(
        client_action: CF,
        peer_action: PF,
        sender_buffer_sz: usize,
        queue_check_timeout: Duration,
    ) where
        CF: FnOnce(mpsc::Sender<Message>, PeerState) -> BoxFuture<'static, ()> + Send + 'static,
        PF: FnOnce(tokio::net::tcp::OwnedReadHalf, StopperCheck) -> BoxFuture<'static, ()>
            + Send
            + 'static,
    {
        let listener = LocalListener::build().await;
        let clistener = listener.clone();

        let (actor, checker) = stopper();
        let cchecker = checker.clone();
        let state = PeerState::new(actor);
        let cstate = state.clone();

        let tpeer = tokio::spawn(async move {
            let stream = clistener.accept().await;
            let (reader, _) = stream.into_split();

            peer_action(reader, cchecker).await;
        });

        let stream = listener.self_connect().await;
        let (_, writer) = stream.into_split();
        let stream_writer = StreamWriter::new(writer);
        let (sender, receiver) = mpsc::channel(sender_buffer_sz);

        spawn_sender(stream_writer, state, checker, receiver, queue_check_timeout);

        let tcli = tokio::spawn(async move {
            client_action(sender, cstate).await;
        });

        assert_ok!(tpeer.await);

        assert_ok!(tcli.await);
    }

    async fn validate_spawn_client_receiver_with_8_pieces_and_69_of_buff_size<CF, PF>(
        client_checker: CF,
        peer_action: PF,
        events: impl Events + 'static,
    ) where
        CF: FnOnce(PeerState, PeerBitfield, PeerAddr) -> BoxFuture<'static, ()> + Send + 'static,
        PF: FnOnce(tokio::net::tcp::OwnedWriteHalf) -> BoxFuture<'static, ()> + Send + 'static,
    {
        let listener = LocalListener::build().await;
        let clistener = listener.clone();

        let (actor, checker) = stopper();
        let state = PeerState::new(actor);
        let cstate = state.clone();

        let peer_addr = listener.addr().into();
        let bitfield = PeerBitfield::new(8);
        let cbitfield = bitfield.clone();

        let tpeer = tokio::spawn(async move {
            let stream = clistener.accept().await;
            let (_, writer) = stream.into_split();

            peer_action(writer).await;
        });

        let stream = listener.self_connect().await;
        let (reader, _) = stream.into_split();
        let stream_reader = StreamReader::new(reader, 69);
        let events = Arc::new(events);

        let trcv =
            spawn_client_receiver(peer_addr, stream_reader, cstate, bitfield, checker, events);

        assert_ok!(trcv.await);

        client_checker(state, cbitfield, peer_addr).await;

        assert_ok!(tpeer.await);
    }

    async fn validate_client<CF, PF>(client_controller: CF, peer_receiver: PF) -> bool
    where
        CF: FnOnce(PeerClient) -> BoxFuture<'static, ()> + Send + 'static,
        PF: FnOnce(
                Arc<Handshake>,
                tokio::net::tcp::OwnedReadHalf,
                tokio::net::tcp::OwnedWriteHalf,
            ) -> BoxFuture<'static, ()>
            + Send
            + 'static,
    {
        let listener = LocalListener::build().await;
        let clistener = listener.clone();

        let torrent: Arc<_> = Torrent {
            tracker: Url::parse("http://localhost:80").unwrap(),
            piece_size: Message::MAX_PIECE_CHUNK_SZ as u64,
            hash_pieces: vec![0; 20].try_into().unwrap(),
            length: 0,
            file_type: FileType::Single {
                name: "".to_string(),
            },
            info_hash: InfoHash([1; 20]),
        }
        .into();
        let peer_id = PeerId::build();
        let handshake = Arc::new(Handshake::new(torrent.info_hash, peer_id));
        let chandshake = handshake.clone();

        let tpeer = tokio::spawn(async move {
            let stream = clistener.accept().await;
            let (reader, writer) = stream.into_split();

            peer_receiver(chandshake, reader, writer).await;
        });

        struct EventsMock;

        impl Events for EventsMock {}

        let senders = Arc::new(EventsMock);
        let stream = listener.self_connect().await;

        let pcli = match PeerClient::start(handshake, stream, torrent, senders).await {
            Some(pcli) => pcli,
            None => return false,
        };

        let tcli = tokio::spawn(async move {
            client_controller(pcli).await;
        });

        assert_ok!(tpeer.await);

        assert_ok!(tcli.await);

        true
    }

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
    fn calc_number_of_bitfield_chunks() {
        [1_u32, 2, 3, 6].iter().for_each(|&total_pieces| {
            assert_eq!(1, bitfield_chunks(total_pieces));
        });

        assert_eq!(4, bitfield_chunks(32));

        assert_eq!(5, bitfield_chunks(35));
    }

    #[test]
    fn calc_bitfield_index() {
        [
            (0, 0, 7),
            (3, 0, 4),
            (8, 1, 7),
            (14, 1, 1),
            (15, 1, 0),
            (17, 2, 6),
        ]
        .iter()
        .for_each(|&(index, chunk_index, offset)| {
            let bindex = BitfieldIndex::new(index);

            assert_eq!(
                bindex.chunk_index, chunk_index,
                "failed with piece index `{}`",
                index
            );
            assert_eq!(bindex.offset, offset, "failed with piece index `{}`", index);
        });
    }

    #[test]
    fn create_empty_peer_bitfield() {
        let bitfield = PeerBitfield::new(42);

        assert_eq!(bitfield.total_pieces, 42);
        assert_eq!(bitfield.num_chunks(), 6);
        let pieces = bitfield.raw();
        assert_eq!(bitfield.num_chunks(), pieces.len() as u32);
        assert!(pieces.iter().all(|&chunk| chunk == 0));
    }

    #[test]
    fn create_peer_bitfield_from_valid_raw_slice() {
        let raw = &[0b10110110, 0b10110111, 0b01010011, 0b10110000];

        let bitfield = assert_some!(PeerBitfield::from_bytes(30, raw));

        assert_eq!(bitfield.total_pieces, 30);
        assert_eq!(bitfield.num_chunks(), 4);
        let pieces = bitfield.raw();
        assert_eq!(bitfield.num_chunks(), pieces.len() as u32);
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
        let expected_pieces = b"\xd9\x0c\x73\x24\x7c\xcb\xfc\xb6\x39\x95".to_vec();
        assert_matches!(msg, Message::Bitfield { pieces } if pieces == expected_pieces);
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
        let expected_block = b"\xd9\x0c\x73\x24\x7c\xcb\xfc\xb6\x39\x95".to_vec();
        assert_matches!(
            msg,
            Message::Piece {
                index: 43,
                begin: 23,
                block,
            } if block == expected_block
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
    fn change_switch() {
        let switch = Switch::new(false);
        assert!(!switch.current());

        switch.set();
        assert!(switch.current());

        switch.unset();
        assert!(!switch.current());
    }

    async fn validate_stopper<F>(action: F)
    where
        F: FnOnce(StopperActor),
    {
        let timeout = Duration::from_secs(4);
        let (actor, check) = stopper();

        let mut c1 = check.clone();
        let t1 = tokio::spawn(async move { tokio::time::timeout(timeout, c1.stopped()).await });

        let mut c2 = check.clone();
        let t2 = tokio::spawn(async move { tokio::time::timeout(timeout, c2.stopped()).await });

        action(actor);

        let rt1 = assert_ok!(t1.await);
        assert_ok!(rt1);

        let rt2 = assert_ok!(t2.await);
        assert_ok!(rt2);
    }

    #[tokio::test]
    async fn stopper_actor_perform_normal_stop() {
        validate_stopper(|actor| {
            actor.stop();
        })
        .await;
    }

    #[tokio::test]
    async fn stopper_actor_closes_channel_on_drop() {
        validate_stopper(|actor| {
            std::mem::drop(actor);
        })
        .await;
    }

    fn new_peer_state() -> PeerState {
        let (actor, _) = stopper();

        PeerState::new(actor)
    }

    #[test]
    fn change_choking_in_peer_state() {
        let state = new_peer_state();

        assert!(state.am_choking_peer());

        state.am_unchoking();
        assert!(!state.am_choking_peer());

        state.am_choking();
        assert!(state.am_choking_peer());
    }

    #[test]
    fn change_interest_in_peer_state() {
        let state = new_peer_state();

        assert!(!state.peer_interested());

        state.peer_interest();
        assert!(state.peer_interested());

        state.peer_uninsterest();
        assert!(!state.peer_interested());
    }

    #[tokio::test]
    async fn mark_peer_as_closed_in_peer_state() {
        let timeout = Duration::from_secs(4);
        let (actor, mut check) = stopper();
        let state = PeerState::new(actor);

        let t = tokio::spawn(async move { tokio::time::timeout(timeout, check.stopped()).await });

        state.close();

        let rt = assert_ok!(t.await);
        assert_ok!(rt);

        assert!(state.closed());
    }

    #[tokio::test]
    async fn stream_reader_reads_message() {
        validate_stream_reader(
            |mut sreader| {
                async move {
                    let msg = match sreader.next_message().await {
                        StreamRead::Received(msg) => msg,
                        other => panic!("StreamReader received: {:?}", other),
                    };

                    assert_matches!(
                        msg,
                        Message::Request {
                            index: 0,
                            begin: 1024,
                            length: 2048,
                        }
                    );
                }
                .boxed()
            },
            |mut writer| {
                async move {
                    let message = Message::Request {
                        index: 0,
                        begin: 1024,
                        length: 2048,
                    };
                    let mut buff = vec![];
                    message.encode(&mut buff);

                    assert_ok!(writer.write_all(&buff).await);
                }
                .boxed()
            },
        )
        .await;
    }

    #[tokio::test]
    async fn stream_reader_reads_message_greater_than_buff() {
        validate_stream_reader(
            |mut sreader| {
                async move {
                    assert_matches!(sreader.next_message().await, StreamRead::Error);
                }
                .boxed()
            },
            |mut writer| {
                async move {
                    assert_ok!(writer.write_u32(Message::MAX_PIECE_CHUNK_SZ + 1).await);
                }
                .boxed()
            },
        )
        .await;
    }

    #[tokio::test]
    async fn stream_reader_did_not_receive_anything_from_supposed_message() {
        validate_stream_reader(
            |mut sreader| {
                async move {
                    assert_matches!(sreader.next_message().await, StreamRead::NotReceived);
                }
                .boxed()
            },
            |_| async move {}.boxed(),
        )
        .await;
    }

    #[tokio::test]
    async fn stream_reader_did_not_receive_message_content() {
        validate_stream_reader(
            |mut sreader| {
                async move {
                    assert_matches!(sreader.next_message().await, StreamRead::NotReceived);
                }
                .boxed()
            },
            |mut writer| {
                async move {
                    assert_ok!(writer.write_u32(8).await);
                }
                .boxed()
            },
        )
        .await;
    }

    #[tokio::test]
    async fn stream_reader_reads_invalid_message() {
        validate_stream_reader(
            |mut sreader| {
                async move {
                    assert_matches!(sreader.next_message().await, StreamRead::Invalid);
                }
                .boxed()
            },
            |mut writer| {
                async move {
                    assert_ok!(writer.write_all(b"\x00\x00\x00\x02\xdf\x23").await);
                }
                .boxed()
            },
        )
        .await;
    }

    #[tokio::test]
    async fn stream_writer_sends_message() {
        validate_stream_writer(
            |mut swriter| {
                async move {
                    let msgs = [
                        Message::Request {
                            index: 43,
                            begin: 23,
                            length: 556,
                        },
                        Message::Unchoke,
                    ];

                    for msg in msgs {
                        assert_ok!(swriter.send(msg).await);
                    }
                }
                .boxed()
            },
            |mut reader| {
                async move {
                    let mut buff = [0; 17];
                    assert_ok!(reader.read_exact(&mut buff).await);
                    assert_eq!(
                        &buff,
                        b"\x00\x00\x00\x0d\x06\x00\x00\x00\x2b\x00\x00\x00\x17\x00\x00\x02\x2c"
                    );

                    let mut buff = [0; 5];
                    assert_ok!(reader.read_exact(&mut buff).await);
                    assert_eq!(&buff, b"\x00\x00\x00\x01\x01");

                    let error = reader.read_u8().await.unwrap_err();
                    assert_matches!(error.kind(), tokio::io::ErrorKind::UnexpectedEof);
                }
                .boxed()
            },
        )
        .await;
    }

    #[tokio::test]
    async fn receive_valid_handshake_as_reply() {
        let accepted = validate_handshake(|handshake, mut reader, mut writer| {
            async move {
                let mut buff = vec![0_u8; handshake.raw().len()];

                assert_ok!(reader.read_exact(buff.as_mut_slice()).await);
                assert_ok!(writer.write_all(buff.as_slice()).await);
            }
            .boxed()
        })
        .await;

        assert!(accepted);
    }

    #[tokio::test]
    async fn receive_invalid_handshake_as_reply() {
        let accepted = validate_handshake(|handshake, mut reader, mut writer| {
            async move {
                let mut buff = vec![0_u8; handshake.raw().len()];

                assert_ok!(reader.read_exact(buff.as_mut_slice()).await);
                assert_ok!(writer.write_u8(1).await);
            }
            .boxed()
        })
        .await;

        assert!(!accepted);
    }

    #[tokio::test]
    async fn connection_is_halted_while_trying_to_send_hanshake() {
        let accepted = validate_handshake(|_, _, _| async move {}.boxed()).await;

        assert!(!accepted);
    }

    #[tokio::test]
    async fn connection_is_halted_while_trying_to_receive_hanshake_reply() {
        let accepted = validate_handshake(|handshake, mut reader, _| {
            async move {
                let mut buff = vec![0_u8; handshake.raw().len()];

                assert_ok!(reader.read_exact(buff.as_mut_slice()).await);
            }
            .boxed()
        })
        .await;

        assert!(!accepted);
    }

    #[tokio::test]
    async fn sender_transmits_messages_to_peer() {
        validate_spawn_sender(
            |sender, _| {
                async move {
                    let msgs = [
                        Message::Unchoke,
                        Message::Request {
                            index: 43,
                            begin: 23,
                            length: 556,
                        },
                        Message::Choke,
                        Message::NotIntersted,
                    ];

                    for msg in msgs {
                        assert_ok!(sender.send(msg).await);
                    }
                }
                .boxed()
            },
            |mut reader, _| {
                async move {
                    let raw_msgs = [
                        b"\x00\x00\x00\x01\x01".to_vec(),
                        b"\x00\x00\x00\x0d\x06\x00\x00\x00\x2b\x00\x00\x00\x17\x00\x00\x02\x2c"
                            .to_vec(),
                        b"\x00\x00\x00\x01\x00".to_vec(),
                        b"\x00\x00\x00\x01\x03".to_vec(),
                    ];

                    for msg in raw_msgs {
                        let mut buffer = vec![0; msg.len()];

                        assert_ok!(
                            reader.read_exact(buffer.as_mut_slice()).await,
                            "failed to read message: {:#04X?}",
                            msg
                        );
                        assert_eq!(msg, buffer);
                    }
                }
                .boxed()
            },
            4,
            Duration::from_secs(3600),
        )
        .await;
    }

    #[tokio::test]
    async fn sender_stops_on_explicit_close() {
        validate_spawn_sender(
            |_, state| {
                async move {
                    state.close();
                }
                .boxed()
            },
            |mut reader, mut checker| {
                async move {
                    checker.stopped().await;

                    let error = reader.read_u8().await.unwrap_err();
                    assert_matches!(error.kind(), tokio::io::ErrorKind::UnexpectedEof);
                }
                .boxed()
            },
            4,
            Duration::from_secs(3600),
        )
        .await;
    }

    #[tokio::test]
    async fn sender_stops_on_connection_closed() {
        validate_spawn_sender(
            |_, _| async move {}.boxed(),
            |reader, mut checker| {
                async move {
                    std::mem::drop(reader);

                    checker.stopped().await;
                }
                .boxed()
            },
            1,
            Duration::from_secs(3600),
        )
        .await;
    }

    #[tokio::test]
    async fn sender_transmits_keep_alive_on_timeout() {
        validate_spawn_sender(
            |sender, _| {
                async move {
                    let _ = sender.send(Message::Unchoke).await;
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
                .boxed()
            },
            |mut reader, _| {
                async move {
                    let raw_msgs = [
                        b"\x00\x00\x00\x01\x01".to_vec(),
                        b"\x00\x00\x00\x00".to_vec(),
                    ];

                    for msg in raw_msgs {
                        let mut buffer = vec![0; msg.len()];

                        assert_ok!(
                            reader.read_exact(buffer.as_mut_slice()).await,
                            "failed to read message: {:#04X?}",
                            msg
                        );
                        assert_eq!(msg, buffer);
                    }
                }
                .boxed()
            },
            1,
            Duration::from_secs(2),
        )
        .await;
    }

    #[tokio::test]
    async fn client_receiver_gets_unchoke_msg() {
        struct EventsMock;

        impl Events for EventsMock {}

        validate_spawn_client_receiver_with_8_pieces_and_69_of_buff_size(
            |state, _, _| {
                async move {
                    assert!(!state.am_choking_peer());
                }
                .boxed()
            },
            |mut writer| {
                async move {
                    assert_ok!(writer.write_all(b"\x00\x00\x00\x01\x01").await);
                }
                .boxed()
            },
            EventsMock,
        )
        .await;
    }

    #[tokio::test]
    async fn client_receiver_gets_choke_msg_after_unchoke_msg() {
        struct EventsMock;

        impl Events for EventsMock {}

        validate_spawn_client_receiver_with_8_pieces_and_69_of_buff_size(
            |state, _, _| {
                async move {
                    assert!(state.am_choking_peer());
                }
                .boxed()
            },
            |mut writer| {
                async move {
                    assert_ok!(writer.write_all(b"\x00\x00\x00\x01\x01").await);
                    assert_ok!(writer.write_all(b"\x00\x00\x00\x01\x00").await);
                }
                .boxed()
            },
            EventsMock,
        )
        .await;
    }

    #[tokio::test]
    async fn client_receiver_gets_interested_msg() {
        struct EventsMock;

        impl Events for EventsMock {}

        validate_spawn_client_receiver_with_8_pieces_and_69_of_buff_size(
            |state, _, _| {
                async move {
                    assert!(state.peer_interested());
                }
                .boxed()
            },
            |mut writer| {
                async move {
                    assert_ok!(writer.write_all(b"\x00\x00\x00\x01\x02").await);
                }
                .boxed()
            },
            EventsMock,
        )
        .await;
    }

    #[tokio::test]
    async fn client_receiver_gets_not_interested_msg_after_interested_msg() {
        struct EventsMock;

        impl Events for EventsMock {}

        validate_spawn_client_receiver_with_8_pieces_and_69_of_buff_size(
            |state, _, _| {
                async move {
                    assert!(!state.peer_interested());
                }
                .boxed()
            },
            |mut writer| {
                async move {
                    assert_ok!(writer.write_all(b"\x00\x00\x00\x01\x02").await);
                    assert_ok!(writer.write_all(b"\x00\x00\x00\x01\x03").await);
                }
                .boxed()
            },
            EventsMock,
        )
        .await;
    }

    #[tokio::test]
    async fn client_receiver_gets_have_msg() {
        struct EventsMock;

        impl Events for EventsMock {}

        validate_spawn_client_receiver_with_8_pieces_and_69_of_buff_size(
            |_, bitfield, _| {
                async move {
                    assert_some_eq!(bitfield.has(3), true);
                }
                .boxed()
            },
            |mut writer| {
                async move {
                    assert_ok!(
                        writer
                            .write_all(b"\x00\x00\x00\x05\x04\x00\x00\x00\x03")
                            .await
                    );
                }
                .boxed()
            },
            EventsMock,
        )
        .await;
    }

    #[tokio::test]
    async fn client_receiver_gets_bitfield_msg() {
        struct EventsMock;

        impl Events for EventsMock {}

        validate_spawn_client_receiver_with_8_pieces_and_69_of_buff_size(
            |_, bitfield, _| {
                async move {
                    assert_eq!(bitfield.raw(), &[0b10010010]);
                }
                .boxed()
            },
            |mut writer| {
                async move {
                    assert_ok!(writer.write_all(b"\x00\x00\x00\x02\x05\x92").await);
                }
                .boxed()
            },
            EventsMock,
        )
        .await;
    }

    #[tokio::test]
    async fn client_receiver_gets_different_bitfield() {
        let (sender, mut receiver) = mpsc::channel(1);

        struct EventsMock;

        impl Events for EventsMock {}

        validate_spawn_client_receiver_with_8_pieces_and_69_of_buff_size(
            |state, _, _| {
                async move {
                    let _ = sender.send(()).await;
                    assert!(state.closed());
                }
                .boxed()
            },
            |mut writer| {
                async move {
                    assert_ok!(writer.write_all(b"\x00\x00\x00\x03\x05\x92\x23").await);
                    receiver.recv().await;
                }
                .boxed()
            },
            EventsMock,
        )
        .await;
    }

    #[tokio::test]
    async fn client_receiver_gets_request_msg() {
        let (sender, mut receiver) = mpsc::channel(1);

        struct EventsMock {
            sender: mpsc::Sender<PieceBlockRequest>,
        }

        impl Events for EventsMock {
            async fn requested_piece(&self, piece_block: PieceBlockRequest) {
                let _ = self.sender.send(piece_block).await;
            }
        }

        validate_spawn_client_receiver_with_8_pieces_and_69_of_buff_size(
            |_, _, peer_addr| {
                async move {
                    let msg = receiver.recv().await.unwrap();

                    assert_eq!(msg.index, 43);
                    assert_eq!(msg.begin, 23);
                    assert_eq!(msg.length, 556);
                    assert_matches!(msg.sender, PieceBlockSender::Client);
                    assert_eq!(msg.peer_addr, peer_addr);
                }
                .boxed()
            },
            |mut writer| {
                async move {
                    assert_ok!(
                        writer
                            .write_all(
                                b"\x00\x00\x00\x0d\x06\x00\x00\x00\
                                \x2b\x00\x00\x00\x17\x00\x00\x02\x2c"
                            )
                            .await
                    );
                }
                .boxed()
            },
            EventsMock { sender },
        )
        .await;
    }

    #[tokio::test]
    async fn client_receiver_gets_piece_msg() {
        let (sender, mut receiver) = mpsc::channel(1);

        struct EventsMock {
            sender: mpsc::Sender<ReceivedPieceBlock>,
        }

        impl Events for EventsMock {
            async fn received_piece_block(&self, piece_block: ReceivedPieceBlock) {
                let _ = self.sender.send(piece_block).await;
            }
        }

        validate_spawn_client_receiver_with_8_pieces_and_69_of_buff_size(
            |_, _, peer_addr| {
                async move {
                    let msg = receiver.recv().await.unwrap();

                    assert_eq!(msg.index, 43);
                    assert_eq!(msg.begin, 23);
                    assert_eq!(msg.piece.0, b"\xd9\x0c\x73\x24\x7c\xcb\xfc\xb6\x39\x95");
                    assert_eq!(msg.peer_addr, peer_addr);
                }
                .boxed()
            },
            |mut writer| {
                async move {
                    assert_ok!(
                        writer
                            .write_all(
                                b"\x00\x00\x00\x13\x07\x00\x00\x00\x2b\x00\x00\
                                \x00\x17\xd9\x0c\x73\x24\x7c\xcb\xfc\xb6\x39\x95"
                            )
                            .await
                    );
                }
                .boxed()
            },
            EventsMock { sender },
        )
        .await;
    }

    #[tokio::test]
    async fn client_receiver_gets_cancel_msg() {
        let (sender, mut receiver) = mpsc::channel(1);

        struct EventsMock {
            sender: mpsc::Sender<CanceledPieceBlock>,
        }

        impl Events for EventsMock {
            async fn canceled_piece(&self, piece_block: CanceledPieceBlock) {
                let _ = self.sender.send(piece_block).await;
            }
        }

        validate_spawn_client_receiver_with_8_pieces_and_69_of_buff_size(
            |_, _, peer_addr| {
                async move {
                    let msg = receiver.recv().await.unwrap();

                    assert_eq!(msg.index, 43);
                    assert_eq!(msg.begin, 23);
                    assert_eq!(msg.length, 556);
                    assert_matches!(msg.sender, PieceBlockSender::Client);
                    assert_eq!(msg.peer_addr, peer_addr);
                }
                .boxed()
            },
            |mut writer| {
                async move {
                    assert_ok!(
                        writer
                            .write_all(
                                b"\x00\x00\x00\x0d\x08\x00\x00\x00\
                                \x2b\x00\x00\x00\x17\x00\x00\x02\x2c"
                            )
                            .await
                    );
                }
                .boxed()
            },
            EventsMock { sender },
        )
        .await;
    }

    #[tokio::test]
    async fn client_receiver_gets_unchoke_keep_alive_msg() {
        struct EventsMock;

        impl Events for EventsMock {}

        validate_spawn_client_receiver_with_8_pieces_and_69_of_buff_size(
            |state, _, _| {
                async move {
                    assert!(!state.am_choking_peer());
                }
                .boxed()
            },
            |mut writer| {
                async move {
                    assert_ok!(writer.write_all(b"\x00\x00\x00\x00").await);
                    assert_ok!(writer.write_all(b"\x00\x00\x00\x01\x01").await);
                }
                .boxed()
            },
            EventsMock,
        )
        .await;
    }

    #[tokio::test]
    async fn client_receiver_gets_msg_with_size_bigger_than_the_buffer() {
        let (sender, mut receiver) = mpsc::channel(1);

        struct EventsMock;

        impl Events for EventsMock {}

        validate_spawn_client_receiver_with_8_pieces_and_69_of_buff_size(
            |_, _, _| {
                async move {
                    let _ = sender.send(()).await;
                }
                .boxed()
            },
            |mut writer| {
                async move {
                    assert_ok!(
                        writer
                            .write_all(
                                b"\x00\x00\x00\x64\x08\x00\x00\x00\
                                \x2b\x00\x00\x00\x17\x00\x00\x02\x2c"
                            )
                            .await
                    );
                    receiver.recv().await;
                }
                .boxed()
            },
            EventsMock,
        )
        .await;
    }

    #[tokio::test]
    async fn client_receiver_does_not_get_the_entire_message() {
        struct EventsMock;

        impl Events for EventsMock {}

        validate_spawn_client_receiver_with_8_pieces_and_69_of_buff_size(
            |_, _, _| async move {}.boxed(),
            |mut writer| {
                async move {
                    assert_ok!(writer.write_all(b"\x00\x00\x00\x05").await);
                }
                .boxed()
            },
            EventsMock,
        )
        .await;
    }

    #[tokio::test]
    async fn client_receiver_gets_unchoke_msg_after_invalid_msg() {
        struct EventsMock;

        impl Events for EventsMock {}

        validate_spawn_client_receiver_with_8_pieces_and_69_of_buff_size(
            |state, _, _| {
                async move {
                    assert!(!state.am_choking_peer());
                }
                .boxed()
            },
            |mut writer| {
                async move {
                    assert_ok!(writer.write_all(b"\x00\x00\x00\x01\xff").await);
                    assert_ok!(writer.write_all(b"\x00\x00\x00\x01\x01").await);
                }
                .boxed()
            },
            EventsMock,
        )
        .await;
    }

    #[tokio::test]
    async fn send_through_client_choke_and_keep_alive_due_to_inactivity() {
        let (sender, mut receiver) = mpsc::channel(1);

        assert!(
            validate_client(
                |peer_client| async move {
                    peer_client.send_message(Message::Choke).await;

                    receiver.recv().await;
                }
                .boxed(),
                |handshake, mut reader, mut writer| async move {
                    let mut buff = vec![0; handshake.raw().len()];
                    assert_ok!(reader.read_exact(&mut buff).await);

                    let received_handshake = assert_some!(Handshake::decode(&buff));
                    assert_eq!(*handshake, received_handshake);

                    assert_ok!(writer.write_all(&buff).await);

                    let mut buff = vec![69; 5];
                    assert_ok!(reader.read_exact(&mut buff).await);
                    assert_eq!(buff, [0, 0, 0, 1, 0]);

                    let mut buff = vec![69; 4];
                    assert_ok!(reader.read_exact(&mut buff).await);
                    assert_eq!(buff, [0, 0, 0, 0]);

                    let _ = sender.send(()).await;
                }
                .boxed(),
            )
            .await
        );
    }

    #[tokio::test]
    async fn client_receives_invalid_handshake() {
        assert!(
            !validate_client(
                |_| async move {}.boxed(),
                |handshake, mut reader, mut writer| async move {
                    let mut buff = vec![0; handshake.raw().len()];
                    assert_ok!(reader.read_exact(&mut buff).await);

                    let received_handshake = assert_some!(Handshake::decode(&buff));
                    assert_eq!(*handshake, received_handshake);

                    assert_ok!(writer.write_all(&[69, 69, 69]).await);
                }
                .boxed(),
            )
            .await
        );
    }
}
