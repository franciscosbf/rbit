use std::{net::SocketAddr, ops::Deref, sync::Arc};

use dashmap::{mapref::multiple::RefMulti, DashMap};

use crate::{Message, PeerClient};

#[derive(Debug)]
pub struct Sender {
    peer: PeerClient,
}

impl Sender {
    async fn send(&self, msg: Message) -> bool {
        !self.peer.am_choking_peer()
            && !self.peer.peer_choking_me()
            && self.peer.send_message(msg).await
    }

    pub fn addr(&self) -> SocketAddr {
        self.peer.addr()
    }

    pub async fn request_piece(&self, index: u32, begin: u32, length: u32) -> bool {
        let msg = Message::Request {
            index,
            begin,
            length,
        };

        self.send(msg).await
    }

    pub async fn send_piece(&self, index: u32, begin: u32, block: Vec<u8>) -> bool {
        let msg = Message::Piece {
            index,
            begin,
            block,
        };

        self.send(msg).await
    }

    pub async fn cancel_request(&self, index: u32, begin: u32, length: u32) -> bool {
        let msg = Message::Cancel {
            index,
            begin,
            length,
        };

        self.send(msg).await
    }

    pub fn has_piece(&self, index: u32) -> Option<bool> {
        self.peer.has_piece(index)
    }
}

impl From<RefMulti<'_, SocketAddr, PeerClient>> for Sender {
    fn from(peer: RefMulti<'_, SocketAddr, PeerClient>) -> Self {
        let peer = peer.clone();

        Sender { peer }
    }
}

impl From<PeerClient> for Sender {
    fn from(peer: PeerClient) -> Self {
        Sender { peer }
    }
}

#[derive(Debug, Clone)]
pub struct Senders {
    peers: Arc<Vec<Sender>>,
}

impl Deref for Senders {
    type Target = [Sender];

    fn deref(&self) -> &Self::Target {
        &self.peers
    }
}

#[derive(Debug)]
pub struct PeersPoolInner {
    peers: DashMap<SocketAddr, PeerClient>,
}

impl PeersPoolInner {
    fn new() -> Self {
        let peers = DashMap::new();

        Self { peers }
    }

    pub fn senders(&self) -> Senders {
        let peers = self
            .peers
            .iter()
            .filter(|peer| !peer.closed() && !peer.am_choking_peer() && !peer.peer_choking_me())
            .map(From::from)
            .collect::<Vec<_>>()
            .into();

        Senders { peers }
    }

    pub fn insert(&self, client: PeerClient) {
        self.peers.insert(client.addr(), client);
    }

    pub fn contains(&self, addr: &SocketAddr) -> bool {
        self.peers.contains_key(addr)
    }

    pub fn remove(&self, addr: &SocketAddr) {
        if let Some((_, peer)) = self.peers.remove(addr) {
            peer.close();
        };
    }

    pub fn purge(&self) {
        self.peers.iter().for_each(|peer| {
            peer.close();
        });

        self.peers.clear();
        self.peers.shrink_to_fit();
    }
}

#[derive(Debug, Clone)]
pub struct PeersPool {
    inner: Arc<PeersPoolInner>,
}

impl PeersPool {
    pub fn new() -> Self {
        let inner = Arc::new(PeersPoolInner::new());

        Self { inner }
    }
}

impl Default for PeersPool {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for PeersPool {
    type Target = PeersPoolInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use std::{io, sync::Arc, time::Duration};

    use futures::channel::oneshot;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    use crate::{
        parse_torrent_file, CanceledPieceBlock, EventsGroup, Handshake, InfoEvents, PeerClient,
        PeerEvents, PeerId, PieceBlockRequest, ReceivedPieceBlock, StatsEvents,
    };

    use super::*;

    struct DummyPeer {
        port: u16,
        stopper: Option<oneshot::Sender<()>>,
        peers: Arc<DashMap<SocketAddr, flume::Sender<Message>>>,
    }

    impl DummyPeer {
        async fn send_to(&self, addr: &SocketAddr, msg: Message) {
            self.peers
                .get(addr)
                .expect("Peer address isn't present")
                .send_async(msg)
                .await
                .expect("Failed to send message to dummy peer");
        }

        async fn close_connection_with(&self, addr: &SocketAddr) {
            self.peers.remove(addr);
        }
    }

    impl Drop for DummyPeer {
        fn drop(&mut self) {
            let _ = self.stopper.take().unwrap().send(());
        }
    }

    async fn start_dummy_peer() -> DummyPeer {
        let listener = TcpListener::bind("localhost:0")
            .await
            .expect("Failed to create TCP listener");
        let port = listener.local_addr().unwrap().port();

        let (sender, mut receiver) = oneshot::channel();

        let peers = Arc::new(DashMap::new());
        let cpeers = peers.clone();

        tokio::spawn(async move {
            loop {
                let (stream, addr) = tokio::select! {
                    re = listener.accept() => {
                        match re {
                            Ok(pair) => pair,
                            Err(_) => continue,
                        }
                    }
                    _ = &mut receiver => return,
                };

                let (msg_sender, msg_receiver) = flume::unbounded();

                cpeers.insert(addr, msg_sender);

                tokio::spawn(async move {
                    let (mut reader, mut writer) = stream.into_split();

                    let pstrlen = reader.read_u8().await?;
                    let mut payload = vec![0; pstrlen as usize + 49];
                    payload[0] = pstrlen;
                    reader.read_exact(&mut payload[1..]).await?;

                    writer.write_all(&payload).await?;

                    tokio::spawn(async move {
                        loop {
                            let size = reader.read_u32().await?;
                            let mut payload = vec![0; size as usize];

                            reader.read_exact(&mut payload).await?;
                        }

                        #[allow(unreachable_code)]
                        io::Result::Ok(())
                    });

                    loop {
                        let msg: Message = match msg_receiver.recv_async().await {
                            Ok(msg) => msg,
                            Err(_) => break,
                        };

                        let mut raw = vec![];
                        msg.encode(&mut raw);

                        writer.write_all(&raw).await?;
                    }

                    #[allow(unreachable_code)]
                    io::Result::Ok(())
                });
            }
        });

        let stopper = Some(sender);

        DummyPeer {
            port,
            stopper,
            peers,
        }
    }

    async fn start_dummy_client<E: PeerEvents>(port: u16, events: Arc<E>) -> PeerClient {
        let raw = b"d8:announce20:https://localhost:804:infod6:lengthi80e4:name\
            4:file12:piece lengthi20e6:pieces80:AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBB\
            AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBee";
        let torrent = parse_torrent_file(raw).unwrap();
        let peer_id = PeerId::build();
        let handshake = Handshake::new(torrent.info_hash, peer_id);
        let stream = TcpStream::connect(format!("localhost:{port}"))
            .await
            .expect("Failed to connect to forwarder");

        PeerClient::start(handshake, stream, torrent, events)
            .await
            .expect("Failed to create peer client")
    }

    async fn wait_with_timeout<T>(
        receiver: &flume::Receiver<T>,
    ) -> Result<Result<T, flume::RecvError>, tokio::time::error::Elapsed> {
        tokio::time::timeout(Duration::from_secs(4), receiver.recv_async()).await
    }

    async fn setup_dummy_and_client_with_stats_events<SE: StatsEvents>(
        events: SE,
    ) -> (DummyPeer, PeerClient) {
        let dummy = start_dummy_peer().await;

        struct InfoEventsMock;
        impl InfoEvents for InfoEventsMock {}
        let events = Arc::new(EventsGroup::new(events, InfoEventsMock));

        let cli = start_dummy_client(dummy.port, events.clone()).await;

        (dummy, cli)
    }

    async fn setup_dummy_and_client_with_info_events<IE: InfoEvents>(
        events: IE,
    ) -> (DummyPeer, PeerClient) {
        let dummy = start_dummy_peer().await;

        struct StatsEventsMock;
        impl StatsEvents for StatsEventsMock {}
        let events = Arc::new(EventsGroup::new(StatsEventsMock, events));

        let cli = start_dummy_client(dummy.port, events.clone()).await;

        (dummy, cli)
    }

    #[tokio::test]
    async fn react_to_choke() {
        let (sender, receiver) = flume::unbounded();

        struct StatsEventsMock {
            sender: flume::Sender<PeerClient>,
        }
        impl StatsEvents for StatsEventsMock {
            async fn on_choke(&self, peer: PeerClient) {
                let _ = self.sender.send_async(peer).await;
            }
        }

        let (dummy, cli) =
            setup_dummy_and_client_with_stats_events(StatsEventsMock { sender }).await;

        dummy.send_to(&cli.addr(), Message::Choke).await;

        let peer = wait_with_timeout(&receiver)
            .await
            .expect("timed out while waiting for on_choke to be invoked")
            .expect("on_choke wasn't invoked");

        assert_eq!(cli.addr(), peer.addr());
    }

    #[tokio::test]
    async fn react_to_unchoke() {
        let (sender, receiver) = flume::unbounded();

        struct StatsEventsMock {
            sender: flume::Sender<PeerClient>,
        }
        impl StatsEvents for StatsEventsMock {
            async fn on_unchoke(&self, peer: PeerClient) {
                let _ = self.sender.send_async(peer).await;
            }
        }

        let (dummy, cli) =
            setup_dummy_and_client_with_stats_events(StatsEventsMock { sender }).await;

        dummy.send_to(&cli.addr(), Message::Unchoke).await;

        let peer = wait_with_timeout(&receiver)
            .await
            .expect("timed out while waiting for on_unchoke to be invoked")
            .expect("on_unchoke wasn't invoked");

        assert_eq!(cli.addr(), peer.addr());
    }

    #[tokio::test]
    async fn react_to_interest() {
        let (sender, receiver) = flume::unbounded();

        struct StatsEventsMock {
            sender: flume::Sender<PeerClient>,
        }
        impl StatsEvents for StatsEventsMock {
            async fn on_interest(&self, peer: PeerClient) {
                let _ = self.sender.send_async(peer).await;
            }
        }

        let (dummy, cli) =
            setup_dummy_and_client_with_stats_events(StatsEventsMock { sender }).await;

        dummy.send_to(&cli.addr(), Message::Interested).await;

        let peer = wait_with_timeout(&receiver)
            .await
            .expect("timed out while waiting for on_interest to be invoked")
            .expect("on_interest wasn't invoked");

        assert_eq!(cli.addr(), peer.addr());
    }

    #[tokio::test]
    async fn react_to_not_interest() {
        let (sender, receiver) = flume::unbounded();

        struct StatsEventsMock {
            sender: flume::Sender<PeerClient>,
        }
        impl StatsEvents for StatsEventsMock {
            async fn on_not_interest(&self, peer: PeerClient) {
                let _ = self.sender.send_async(peer).await;
            }
        }

        let (dummy, cli) =
            setup_dummy_and_client_with_stats_events(StatsEventsMock { sender }).await;

        dummy.send_to(&cli.addr(), Message::NotInterested).await;

        let peer = wait_with_timeout(&receiver)
            .await
            .expect("timed out while waiting for on_not_interest to be invoked")
            .expect("on_not_interest wasn't invoked");

        assert_eq!(cli.addr(), peer.addr());
    }

    #[tokio::test]
    async fn react_to_implicit_close() {
        let (sender, receiver) = flume::unbounded();

        struct StatsEventsMock {
            sender: flume::Sender<PeerClient>,
        }
        impl StatsEvents for StatsEventsMock {
            async fn on_implicit_close(&self, peer: PeerClient) {
                let _ = self.sender.send_async(peer).await;
            }
        }

        let (dummy, cli) =
            setup_dummy_and_client_with_stats_events(StatsEventsMock { sender }).await;

        dummy.close_connection_with(&cli.addr()).await;

        let peer = wait_with_timeout(&receiver)
            .await
            .expect("timed out while waiting for on_implicit_close to be invoked")
            .expect("on_implicit_close wasn't invoked");

        assert_eq!(cli.addr(), peer.addr());
    }

    #[tokio::test]
    async fn react_to_piece_block_request() {
        let (sender, receiver) = flume::unbounded();

        struct InfoEventsMock {
            sender: flume::Sender<(PeerClient, PieceBlockRequest)>,
        }
        impl InfoEvents for InfoEventsMock {
            async fn on_piece_block_request(
                &self,
                client: PeerClient,
                piece_block: PieceBlockRequest,
            ) {
                let _ = self.sender.send_async((client, piece_block)).await;
            }
        }

        let (dummy, cli) = setup_dummy_and_client_with_info_events(InfoEventsMock { sender }).await;

        cli.send_message(Message::Unchoke).await;

        dummy
            .send_to(
                &cli.addr(),
                Message::Request {
                    index: 1,
                    begin: 10,
                    length: 20,
                },
            )
            .await;

        let (client, piece_block) = wait_with_timeout(&receiver)
            .await
            .expect("timed out while waiting for on_piece_block_request to be invoked")
            .expect("on_piece_block_request wasn't invoked");

        assert_eq!(cli.addr(), client.addr());
        assert_eq!(piece_block.index, 1);
        assert_eq!(piece_block.begin, 10);
        assert_eq!(piece_block.length, 20);
    }

    #[tokio::test]
    async fn react_to_received_piece_block() {
        let (sender, receiver) = flume::unbounded();

        struct InfoEventsMock {
            sender: flume::Sender<(PeerClient, ReceivedPieceBlock)>,
        }
        impl InfoEvents for InfoEventsMock {
            async fn on_received_piece_block(
                &self,
                client: PeerClient,
                piece_block: ReceivedPieceBlock,
            ) {
                let _ = self.sender.send_async((client, piece_block)).await;
            }
        }

        let (dummy, cli) = setup_dummy_and_client_with_info_events(InfoEventsMock { sender }).await;

        cli.send_message(Message::Unchoke).await;

        dummy
            .send_to(
                &cli.addr(),
                Message::Piece {
                    index: 3,
                    begin: 0,
                    block: vec![1; 20],
                },
            )
            .await;

        let (client, piece_block) = wait_with_timeout(&receiver)
            .await
            .expect("timed out while waiting for on_received_piece_block to be invoked")
            .expect("on_received_piece_block wasn't invoked");

        assert_eq!(cli.addr(), client.addr());
        assert_eq!(piece_block.index, 3);
        assert_eq!(piece_block.begin, 0);
        assert_eq!(piece_block.block, vec![1; 20]);
    }

    #[tokio::test]
    async fn react_to_canceled_piece_block() {
        let (sender, receiver) = flume::unbounded();

        struct InfoEventsMock {
            sender: flume::Sender<(PeerClient, CanceledPieceBlock)>,
        }
        impl InfoEvents for InfoEventsMock {
            async fn on_canceled_piece_block(
                &self,
                client: PeerClient,
                piece_block: CanceledPieceBlock,
            ) {
                let _ = self.sender.send_async((client, piece_block)).await;
            }
        }

        let (dummy, cli) = setup_dummy_and_client_with_info_events(InfoEventsMock { sender }).await;

        cli.send_message(Message::Unchoke).await;

        dummy
            .send_to(
                &cli.addr(),
                Message::Cancel {
                    index: 1,
                    begin: 10,
                    length: 20,
                },
            )
            .await;

        let (client, piece_block) = wait_with_timeout(&receiver)
            .await
            .expect("timed out while waiting for on_canceled_piece_block to be invoked")
            .expect("on_canceled_piece_block wasn't invoked");

        assert_eq!(cli.addr(), client.addr());
        assert_eq!(piece_block.index, 1);
        assert_eq!(piece_block.begin, 10);
        assert_eq!(piece_block.length, 20);
    }

    #[tokio::test]
    async fn add_and_remove_peers() {
        let dummy = start_dummy_peer().await;

        let peers = PeersPool::new();

        let (unchoke_sender, unchoke_receiver) = flume::unbounded();
        let (choke_sender, choke_receiver) = flume::unbounded();

        struct StatsEventsMock {
            unchoke_sender: flume::Sender<PeerClient>,
            choke_sender: flume::Sender<PeerClient>,
        }
        impl StatsEvents for StatsEventsMock {
            async fn on_unchoke(&self, peer: PeerClient) {
                let _ = self.unchoke_sender.send_async(peer).await;
            }

            async fn on_choke(&self, peer: PeerClient) {
                let _ = self.choke_sender.send_async(peer).await;
            }
        }
        struct InfoEventsMock;
        impl InfoEvents for InfoEventsMock {}
        let events = Arc::new(EventsGroup::new(
            StatsEventsMock {
                unchoke_sender,
                choke_sender,
            },
            InfoEventsMock,
        ));

        let cl1 = start_dummy_client(dummy.port, events.clone()).await;
        let cl2 = start_dummy_client(dummy.port, events.clone()).await;
        let cl3 = start_dummy_client(dummy.port, events.clone()).await;
        let cl4 = start_dummy_client(dummy.port, events.clone()).await;

        peers.insert(cl1.clone());
        peers.insert(cl2.clone());
        peers.insert(cl3.clone());
        peers.insert(cl4.clone());

        assert!(peers.senders().is_empty());

        cl1.send_message(Message::Unchoke).await;
        cl2.send_message(Message::Unchoke).await;
        cl3.send_message(Message::Unchoke).await;
        cl4.send_message(Message::Unchoke).await;

        dummy.send_to(&cl1.addr(), Message::Unchoke).await;
        dummy.send_to(&cl2.addr(), Message::Unchoke).await;
        dummy.send_to(&cl3.addr(), Message::Unchoke).await;
        dummy.send_to(&cl4.addr(), Message::Unchoke).await;

        let mut received_peers = vec![];
        for npeers in 0..4 {
            let peer = wait_with_timeout(&unchoke_receiver)
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "Timed out while waiting for on_unchoke to be invoked. \
                            Received {npeers} peers from on_unchoke"
                    )
                })
                .expect("on_unchoke wasn't invoked");

            received_peers.push(peer);
        }

        assert!(received_peers.iter().any(|peer| peer.addr() == cl1.addr()));
        assert!(received_peers.iter().any(|peer| peer.addr() == cl2.addr()));
        assert!(received_peers.iter().any(|peer| peer.addr() == cl3.addr()));
        assert!(received_peers.iter().any(|peer| peer.addr() == cl4.addr()));

        let senders = peers.senders();

        assert_eq!(senders.len(), 4);
        assert!(senders.iter().any(|sender| sender.addr() == cl1.addr()));
        assert!(senders.iter().any(|sender| sender.addr() == cl2.addr()));
        assert!(senders.iter().any(|sender| sender.addr() == cl3.addr()));
        assert!(senders.iter().any(|sender| sender.addr() == cl4.addr()));

        peers.insert(cl1.clone());
        peers.insert(cl2.clone());
        peers.insert(cl3.clone());
        peers.insert(cl4.clone());

        assert!(peers.contains(&cl1.addr()));
        assert!(peers.contains(&cl2.addr()));
        assert!(peers.contains(&cl3.addr()));
        assert!(peers.contains(&cl4.addr()));

        peers.remove(&cl4.addr());

        assert!(!peers.contains(&cl4.addr()));

        cl1.close();

        let senders = peers.senders();

        assert_eq!(senders.len(), 2);
        assert!(senders.iter().any(|sender| sender.addr() == cl2.addr()));
        assert!(senders.iter().any(|sender| sender.addr() == cl3.addr()));
        assert!(peers.contains(&cl1.addr()));
        assert!(peers.contains(&cl2.addr()));
        assert!(peers.contains(&cl3.addr()));

        cl2.send_message(Message::Choke).await;

        let senders = peers.senders();

        assert_eq!(senders.len(), 1);
        assert_eq!(senders[0].addr(), cl3.addr());
        assert!(peers.contains(&cl1.addr()));
        assert!(peers.contains(&cl2.addr()));
        assert!(peers.contains(&cl3.addr()));

        dummy.send_to(&cl3.addr(), Message::Choke).await;

        let peer = wait_with_timeout(&choke_receiver)
            .await
            .expect("timed out while waiting for on_choke to be invoked")
            .expect("on_choke wasn't invoked");

        assert_eq!(peer.addr(), cl3.addr());

        let senders = peers.senders();

        assert!(senders.is_empty());
        assert!(peers.contains(&cl1.addr()));
        assert!(peers.contains(&cl2.addr()));
        assert!(peers.contains(&cl3.addr()));

        peers.purge();

        assert!(!peers.contains(&cl1.addr()));
        assert!(!peers.contains(&cl2.addr()));
        assert!(!peers.contains(&cl3.addr()));
    }
}
