use std::{net::SocketAddr, ops::Deref, sync::Arc};

use dashmap::{mapref::multiple::RefMulti, DashMap};

use crate::{
    CanceledPieceBlock, Message, PeerClient, PeerEvents, PieceBlockRequest, ReceivedPieceBlock,
};

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

#[allow(unused_variables)]
#[trait_variant::make(Send + Sync)]
pub trait StatsEvents: 'static {
    async fn on_choke(&self, peers: PeersPool, peer: PeerClient) {
        async {}
    }
    async fn on_unchoke(&self, peers: PeersPool, peer: PeerClient) {
        async {}
    }
    async fn on_interest(&self, peers: PeersPool, peer: PeerClient) {
        async {}
    }
    async fn on_not_interest(&self, peers: PeersPool, peer: PeerClient) {
        async {}
    }
    async fn on_implicit_close(&self, peers: PeersPool, peer: PeerClient) {
        async {}
    }
}

#[allow(unused_variables)]
#[trait_variant::make(Send + Sync)]
pub trait InfoEvents: 'static {
    async fn on_piece_block_request(&self, sender: Sender, piece_block: PieceBlockRequest) {
        async {}
    }
    async fn on_received_piece_block(&self, sender: Sender, piece_block: ReceivedPieceBlock) {
        async {}
    }
    async fn on_canceled_piece_block(&self, sender: Sender, piece_block: CanceledPieceBlock) {
        async {}
    }
}

pub struct EventsGroup<IE: InfoEvents, SE: StatsEvents> {
    peers: PeersPool,
    stats: SE,
    info: IE,
}

impl<IE: InfoEvents, SE: StatsEvents> EventsGroup<IE, SE> {
    pub fn new(peers: PeersPool, stats: SE, info: IE) -> Self {
        Self { peers, stats, info }
    }
}

impl<IE: InfoEvents, SE: StatsEvents> PeerEvents for EventsGroup<IE, SE> {
    async fn on_choke(&self, peer: PeerClient) {
        self.stats.on_choke(self.peers.clone(), peer).await;
    }

    async fn on_unchoke(&self, peer: PeerClient) {
        self.stats.on_unchoke(self.peers.clone(), peer).await;
    }

    async fn on_interest(&self, peer: PeerClient) {
        self.stats.on_interest(self.peers.clone(), peer).await;
    }

    async fn on_not_interest(&self, peer: PeerClient) {
        self.stats.on_not_interest(self.peers.clone(), peer).await;
    }

    async fn on_implicit_close(&self, peer: PeerClient) {
        self.stats.on_implicit_close(self.peers.clone(), peer).await;
    }

    async fn on_piece_block_request(&self, peer: PeerClient, piece_block: PieceBlockRequest) {
        self.info
            .on_piece_block_request(peer.into(), piece_block)
            .await;
    }

    async fn on_received_piece_block(&self, peer: PeerClient, piece_block: ReceivedPieceBlock) {
        self.info
            .on_received_piece_block(peer.into(), piece_block)
            .await;
    }

    async fn on_canceled_piece_block(&self, peer: PeerClient, piece_block: CanceledPieceBlock) {
        self.info
            .on_canceled_piece_block(peer.into(), piece_block)
            .await;
    }
}

#[cfg(test)]
mod tests {
    // TODO:
}
