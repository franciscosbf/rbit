use crate::{CanceledPieceBlock, PeerClient, PeerEvents, PieceBlockRequest, ReceivedPieceBlock};

#[allow(unused_variables)]
#[trait_variant::make(Send + Sync)]
pub trait StatsEvents: 'static {
    async fn on_choke(&self, peer: PeerClient) {
        async {}
    }
    async fn on_unchoke(&self, peer: PeerClient) {
        async {}
    }
    async fn on_interest(&self, peer: PeerClient) {
        async {}
    }
    async fn on_not_interest(&self, peer: PeerClient) {
        async {}
    }
    async fn on_implicit_close(&self, peer: PeerClient) {
        async {}
    }
}

#[allow(unused_variables)]
#[trait_variant::make(Send + Sync)]
pub trait InfoEvents: 'static {
    async fn on_piece_block_request(&self, client: PeerClient, piece_block: PieceBlockRequest) {
        async {}
    }
    async fn on_received_piece_block(&self, client: PeerClient, piece_block: ReceivedPieceBlock) {
        async {}
    }
    async fn on_canceled_piece_block(&self, client: PeerClient, piece_block: CanceledPieceBlock) {
        async {}
    }
}

pub struct EventsGroup<IE: InfoEvents, SE: StatsEvents> {
    stats: SE,
    info: IE,
}

impl<IE: InfoEvents, SE: StatsEvents> EventsGroup<IE, SE> {
    pub fn new(stats: SE, info: IE) -> Self {
        Self { stats, info }
    }
}

impl<IE: InfoEvents, SE: StatsEvents> PeerEvents for EventsGroup<IE, SE> {
    async fn on_choke(&self, peer: PeerClient) {
        self.stats.on_choke(peer).await;
    }

    async fn on_unchoke(&self, peer: PeerClient) {
        self.stats.on_unchoke(peer).await;
    }

    async fn on_interest(&self, peer: PeerClient) {
        self.stats.on_interest(peer).await;
    }

    async fn on_not_interest(&self, peer: PeerClient) {
        self.stats.on_not_interest(peer).await;
    }

    async fn on_implicit_close(&self, peer: PeerClient) {
        self.stats.on_implicit_close(peer).await;
    }

    async fn on_piece_block_request(&self, peer: PeerClient, piece_block: PieceBlockRequest) {
        self.info.on_piece_block_request(peer, piece_block).await;
    }

    async fn on_received_piece_block(&self, peer: PeerClient, piece_block: ReceivedPieceBlock) {
        self.info.on_received_piece_block(peer, piece_block).await;
    }

    async fn on_canceled_piece_block(&self, peer: PeerClient, piece_block: CanceledPieceBlock) {
        self.info.on_canceled_piece_block(peer, piece_block).await;
    }
}

#[cfg(test)]
mod tests {
    use std::{io, net::SocketAddr, sync::Arc, time::Duration};

    use dashmap::DashMap;
    use futures::channel::oneshot;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    use crate::{
        parse_torrent_file, CanceledPieceBlock, Handshake, Message, PeerClient, PeerEvents, PeerId,
        PieceBlockRequest, ReceivedPieceBlock,
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
}
