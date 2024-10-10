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
