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
    fn as_str(&self) -> &str {
        match self {
            Event::Started => "started",
            Event::Completed => "completed",
            Event::Stopped => "stopped",
            Event::Empty => "empty",
        }
    }
}

#[derive(Debug)]
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

#[derive(Debug)]
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
        let port = u16::from_be_bytes([raw[4], raw[5]]);
        let saddr_ipv4 = SocketAddrV4::new(ipv4, port);

        Ok(SocketAddr::V4(saddr_ipv4).into())
    }
}

#[derive(Debug)]
pub struct Peers {
    pub interval: Interval,
    pub addresses: Vec<PeerAddr>,
}

impl Peers {
    fn new(interval: Interval, addresses: Vec<PeerAddr>) -> Self {
        Self {
            interval,
            addresses,
        }
    }
}

impl TryFrom<TrackerResponse> for Peers {
    type Error = RbitError;

    fn try_from(response: TrackerResponse) -> Result<Self, Self::Error> {
        match (response.failure, response.success) {
            (Some(error), _) => Err(RbitError::TrackerErrorResponse(error)),
            (None, Some(msg)) => {
                let interval = msg.interval.try_into()?;

                let addresses = match msg.peers {
                    PeersFormat::Simple(ready) => ready
                        .into_iter()
                        .map(|peer| peer.try_into())
                        .collect::<Result<_, RbitError>>()?,
                    PeersFormat::Compact(raw) => {
                        const CPEER_SZ: usize = 6;

                        if raw.len() % CPEER_SZ != 0 {
                            return Err(RbitError::InvalidPeers("invalid peers compact format"));
                        }

                        (0..raw.len())
                            .step_by(CPEER_SZ)
                            .map(|i| (&raw[i..i + CPEER_SZ]).try_into())
                            .collect::<Result<_, RbitError>>()?
                    }
                };

                Ok(Peers::new(interval, addresses))
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
        peer_id: &PeerId,
        timeout: Duration,
    ) -> Self {
        let http_client = reqwest::Client::builder().timeout(timeout).build().unwrap();

        let port = listening_port.to_string();
        let peer_id = encode_query_value(peer_id);

        base_tracker_url
            .query_pairs_mut()
            .clear()
            .append_pair("compat", "1")
            .append_pair("port", &port)
            .append_pair("peer_id", &peer_id);

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
        let info_hash = encode_query_value(&info_hash);

        let response = self
            .http_client
            .get(self.base_tracker_url.as_str())
            .query(&[("info_hash", &info_hash)])
            .query(&[("uploaded", uploaded)])
            .query(&[("downloaded", downloaded)])
            .query(&[("left", left)])
            .query(&[("event", event.as_str())])
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
    use std::{
        net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
        time::Duration,
    };

    use claims::{assert_matches, assert_ok};
    use serde_bytes::ByteBuf;

    use crate::{error::RbitError, PeerAddr};

    use super::{encode_query_value, Interval, Peer, Peers, PeersFormat, Success, TrackerResponse};

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

    #[test]
    fn parse_valid_interval() {
        let interval: Interval = assert_ok!(1_i64.try_into());

        assert_eq!(*interval, Duration::from_secs(1));
    }

    #[test]
    fn parse_invalid_interval() {
        assert_matches!(
            <i64 as TryInto<Interval>>::try_into(-1 as i64),
            Err(RbitError::InvalidPeers("invalid interval"))
        );
    }

    #[test]
    fn convert_valid_peer_to_peer_addr() {
        let paddr: PeerAddr = assert_ok!(Peer {
            ip: "192.145.124.34".into(),
            port: 45,
        }
        .try_into());

        assert_eq!(
            *paddr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 145, 124, 34)), 45)
        );

        let paddr: PeerAddr = assert_ok!(Peer {
            ip: "[0:0:0:0:0:ffff:c00a:2ff]".into(),
            port: 45,
        }
        .try_into());

        assert_eq!(
            *paddr,
            SocketAddr::new(
                IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0xc00a, 0x2ff)),
                45
            )
        );

        assert_ok!(<Peer as TryInto<PeerAddr>>::try_into(Peer {
            ip: "localhost".into(),
            port: 45,
        }));
    }

    #[test]
    fn convert_invalid_peer_to_peer_addr() {
        assert_matches!(
            <Peer as TryInto<PeerAddr>>::try_into(Peer {
                ip: "192.145.--.34".into(),
                port: 45,
            }),
            Err(RbitError::InvalidPeers("invalid peer ip"))
        );

        assert_matches!(
            <Peer as TryInto<PeerAddr>>::try_into(Peer {
                ip: "[0:0:0:00:ffff:c00a:2ff]".into(),
                port: 45,
            }),
            Err(RbitError::InvalidPeers("invalid peer ip"))
        );

        assert_matches!(
            <Peer as TryInto<PeerAddr>>::try_into(Peer {
                ip: "localhost.invalid".into(),
                port: 45,
            }),
            Err(RbitError::InvalidPeers("domain lookup failed"))
        );

        // WARN: idk how to test domain resolution result without returned ips.
    }

    #[test]
    fn convert_byte_slice_to_peer_addr() {
        let paddr: PeerAddr = assert_ok!((&[192_u8, 145, 21, 34, 0, 45][..]).try_into());

        assert_eq!(
            *paddr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 145, 21, 34)), 45)
        );
    }

    #[test]
    fn convert_valid_tracker_response_with_simple_peers_format_to_peers() {
        let tracker_response = TrackerResponse {
            failure: None,
            success: Some(Success {
                interval: 2,
                peers: PeersFormat::Simple(vec![
                    Peer {
                        ip: "192.145.124.34".into(),
                        port: 45,
                    },
                    Peer {
                        ip: "[0:0:0:0:0:ffff:c00a:2ff]".into(),
                        port: 124,
                    },
                ]),
            }),
        };

        let peers: Peers = assert_ok!(tracker_response.try_into());
        assert_eq!(*peers.interval, Duration::from_secs(2));
        assert_eq!(peers.addresses.len(), 2);
        assert_eq!(
            *peers.addresses[0],
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 145, 124, 34)), 45)
        );
        assert_eq!(
            *peers.addresses[1],
            SocketAddr::new(
                IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0xc00a, 0x2ff)),
                124
            )
        );
    }

    #[test]
    fn convert_valid_tracker_response_with_compact_peers_format_to_peers() {
        let tracker_response = TrackerResponse {
            failure: None,
            success: Some(Success {
                interval: 2,
                peers: PeersFormat::Compact(ByteBuf::from::<&[u8]>(&[
                    192, 145, 21, 34, 0, 45, 54, 225, 23, 1, 17, 82,
                ])),
            }),
        };

        let peers: Peers = assert_ok!(tracker_response.try_into());
        assert_eq!(*peers.interval, Duration::from_secs(2));
        assert_eq!(peers.addresses.len(), 2);
        assert_eq!(
            *peers.addresses[0],
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 145, 21, 34)), 45)
        );
        assert_eq!(
            *peers.addresses[1],
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(54, 225, 23, 1)), 4434)
        );
    }

    #[test]
    fn convert_valid_tracker_response_with_error_message() {
        let _error = RbitError::TrackerErrorResponse("test".into());

        assert_matches!(
            <TrackerResponse as TryInto<Peers>>::try_into(TrackerResponse {
                failure: Some("test".into()),
                success: None,
            }),
            Err(_error)
        );
    }

    #[test]
    fn convert_invalid_tracker_response_format() {
        assert_matches!(
            <TrackerResponse as TryInto<Peers>>::try_into(TrackerResponse {
                failure: None,
                success: None,
            }),
            Err(RbitError::InvalidPeers("unexpected format"))
        );
    }

    #[test]
    fn convert_invalid_tracker_response_with_invalid_peers_compat_format() {
        assert_matches!(
            <TrackerResponse as TryInto<Peers>>::try_into(TrackerResponse {
                failure: None,
                success: Some(Success {
                    interval: 2,
                    peers: PeersFormat::Compact(ByteBuf::from::<&[u8]>(&[192, 145, 21, 34, 0])),
                }),
            }),
            Err(RbitError::InvalidPeers("invalid peers compact format"))
        );
    }
}
