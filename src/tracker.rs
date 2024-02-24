use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, ToSocketAddrs},
    ops::Deref,
    time::Duration,
};

use serde_bytes::ByteBuf;
use url::{Host, Url};

use crate::{error::RbitError, PeerId, Piece};

fn encode_query_value(bts: &[u8]) -> String {
    let mut encoded = String::with_capacity(bts.len());

    bts.iter().for_each(|&b| {
        if b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_' | b'~') {
            encoded.push(b as char);
        } else {
            const HEX: &[u8] = b"0123456789ABCDEF";
            encoded.push('%');
            encoded.push(HEX[(b as usize) >> 4 & 0xF] as char);
            encoded.push(HEX[(b as usize) & 0xF] as char);
        }
    });

    encoded
}

#[derive(Debug, serde::Deserialize)]
struct Peer {
    ip: String,
    port: i64,
}

#[derive(Debug, serde::Deserialize)]
#[serde(untagged)]
enum PeersFormat {
    Simple(Vec<Peer>),
    Compact(ByteBuf),
}

#[derive(Debug, serde::Deserialize)]
struct Success {
    interval: i64,
    peers: PeersFormat,
}

#[derive(Debug, serde::Deserialize)]
struct TrackerResponse {
    failure: Option<String>,
    #[serde(flatten)]
    success: Option<Success>,
}

pub enum Event {
    Started,
    Completed,
    Stopped,
    Empty,
}

impl Event {
    fn to_str(&self) -> &str {
        match self {
            Event::Started => "started",
            Event::Completed => "completed",
            Event::Stopped => "stopped",
            Event::Empty => "empty",
        }
    }
}

pub struct Interval(Duration);

impl Deref for Interval {
    type Target = Duration;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<i64> for Interval {
    type Error = RbitError;

    fn try_from(raw: i64) -> Result<Self, Self::Error> {
        if raw >= 0 {
            Ok(Interval(Duration::from_secs(raw as u64)))
        } else {
            Err(RbitError::InvalidPeers("invalid interval"))
        }
    }
}

pub struct PeerAddr(SocketAddr);

impl From<SocketAddr> for PeerAddr {
    fn from(value: SocketAddr) -> Self {
        PeerAddr(value)
    }
}

impl Deref for PeerAddr {
    type Target = SocketAddr;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<Peer> for PeerAddr {
    type Error = RbitError;

    fn try_from(peer: Peer) -> Result<Self, Self::Error> {
        let port = peer.port as u16;
        let ip =
            match Host::parse(&peer.ip).map_err(|_| RbitError::InvalidPeers("invalid peer ip"))? {
                Host::Domain(domain) => (domain, 0)
                    .to_socket_addrs()
                    .map_err(|_| RbitError::InvalidPeers("domain lookup failed"))?
                    .next()
                    .map(|saddr| saddr.ip())
                    .ok_or(RbitError::InvalidPeers("unknown domain ip"))?,
                Host::Ipv4(ip) => IpAddr::V4(ip),
                Host::Ipv6(ip) => IpAddr::V6(ip),
            };

        Ok(SocketAddr::new(ip, port).into())
    }
}

impl TryFrom<&[u8]> for PeerAddr {
    type Error = RbitError;

    fn try_from(raw: &[u8]) -> Result<Self, Self::Error> {
        let ipv4 = Ipv4Addr::new(raw[0], raw[1], raw[2], raw[3]);
        let port = ((raw[4] as u16) << 8) | raw[5] as u16;
        let saddr_ipv4 = SocketAddrV4::new(ipv4, port);

        Ok(SocketAddr::V4(saddr_ipv4).into())
    }
}

pub struct Peers {
    pub interval: Interval,
    pub peers: Vec<PeerAddr>,
}

impl Peers {
    fn new(interval: Interval, peers: Vec<PeerAddr>) -> Self {
        Self { interval, peers }
    }
}

impl TryFrom<TrackerResponse> for Peers {
    type Error = RbitError;

    fn try_from(response: TrackerResponse) -> Result<Self, Self::Error> {
        match (response.failure, response.success) {
            (Some(error), _) => Err(RbitError::TrackerError(error)),
            (None, Some(msg)) => {
                let interval = msg.interval.try_into()?;

                let peers = match msg.peers {
                    PeersFormat::Simple(ready) => ready
                        .into_iter()
                        .map(|peer| peer.try_into())
                        .collect::<Result<_, RbitError>>()?,
                    PeersFormat::Compact(raw) => {
                        const CPEER_SZ: usize = 6;

                        if raw.len() % CPEER_SZ != 0 {
                            return Err(RbitError::InvalidPeers("invalid peer compact format"));
                        }

                        (0..raw.len())
                            .step_by(CPEER_SZ)
                            .map(|i| (&raw[i..i + CPEER_SZ]).try_into())
                            .collect::<Result<_, RbitError>>()?
                    }
                };

                Ok(Peers::new(interval, peers))
            }
            _ => Err(RbitError::InvalidPeers("unexpected format")),
        }
    }
}

pub struct TrackerClient {
    http_client: reqwest::Client,
    base_tracker_url: Url,
}

impl TrackerClient {
    pub fn new(
        mut base_tracker_url: Url,
        listening_port: u16,
        peer_id: PeerId,
        timeout: Duration,
    ) -> Self {
        let http_client = reqwest::Client::builder().timeout(timeout).build().unwrap();

        base_tracker_url
            .query_pairs_mut()
            .clear()
            .append_pair("compat", "1")
            .append_pair("port", &listening_port.to_string())
            .append_pair("peer_id", &encode_query_value(&peer_id));

        Self {
            http_client,
            base_tracker_url,
        }
    }

    pub async fn fetch_peers(
        &self,
        info_hash: Piece<'_>,
        uploaded: usize,
        downloaded: usize,
        left: usize,
        event: Event,
    ) -> Result<Peers, RbitError> {
        let response = self
            .http_client
            .get(self.base_tracker_url.as_str())
            .query(&[("info_hash", &encode_query_value(&info_hash))])
            .query(&[("uploaded", uploaded)])
            .query(&[("downloaded", downloaded)])
            .query(&[("left", left)])
            .query(&[("event", event.to_str())])
            .send()
            .await
            .map_err(RbitError::TrackerFailed)?;

        let body = response.bytes().await.map_err(RbitError::TrackerFailed)?;

        serde_bencode::from_bytes::<TrackerResponse>(&body)
            .map_err(|_| RbitError::InvalidPeers("unexpected format"))?
            .try_into()
    }
}

#[cfg(test)]
mod tests {
    use super::encode_query_value;

    #[test]
    fn encode_piece_sha1() {
        let sha1 = &[
            0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF1, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD,
            0xEF, 0x12, 0x34, 0x56, 0x78, 0x9A,
        ];

        assert_eq!(
            "%124Vx%9A%BC%DE%F1%23Eg%89%AB%CD%EF%124Vx%9A",
            encode_query_value(sha1)
        );

        let normal_chars = &[b'a', b'Z', b'1', b'.', b'-', b'_', b'~'];

        assert_eq!("aZ1.-_~", encode_query_value(normal_chars));
    }
}
