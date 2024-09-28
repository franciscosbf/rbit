use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, ToSocketAddrs},
    ops::Deref,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use bendy::decoding::{Error, FromBencode, Object};
use thiserror::Error;
use url::{Host, Url};

use crate::{InfoHash, PeerId};

#[derive(Error, Debug)]
pub enum TrackerError {
    #[error("error trying get peers: {0}")]
    RequestFailed(#[from] reqwest::Error),
    #[error("invalid tracker response")]
    InvalidTrackerResponse,
    #[error("peers parser error: {0}")]
    PeersParserFailed(&'static str),
    #[error("tracker error: {0}")]
    TrackerErrorResponse(String),
}

fn bytes_to_str(raw: &[u8]) -> &str {
    unsafe { std::str::from_utf8_unchecked(raw) }
}

#[derive(Debug)]
struct Peer {
    ip: String,
    port: u16,
}

impl FromBencode for Peer {
    const EXPECTED_RECURSION_DEPTH: usize = 1;

    fn decode_bencode_object(object: Object) -> Result<Self, Error>
    where
        Self: Sized,
    {
        let mut ip = None;
        let mut port = None;

        let mut dict = object.try_into_dictionary()?;
        while let Some(pair) = dict.next_pair()? {
            match pair {
                (b"ip", value) => {
                    ip = String::decode_bencode_object(value).map(Some)?;
                }
                (b"port", value) => {
                    let raw_port = value.try_into_integer()?;

                    port = u16::from_str(raw_port)
                        .map(Some)
                        .map_err(Error::malformed_content)?;
                }
                _ => (),
            }
        }

        let ip = ip.ok_or_else(|| Error::missing_field("ip"))?;
        let port = port.ok_or_else(|| Error::missing_field("port"))?;

        Ok(Peer { ip, port })
    }
}

#[derive(Debug)]
enum PeersFormat {
    Simple(Vec<Peer>),
    Compact(Vec<u8>),
}

#[derive(Debug)]
struct Success {
    interval: u64,
    peers: PeersFormat,
}

#[derive(Debug)]
struct TrackerResponse {
    failure: Option<String>,
    success: Option<Success>,
}

impl FromBencode for TrackerResponse {
    const EXPECTED_RECURSION_DEPTH: usize = Peer::EXPECTED_RECURSION_DEPTH + 2;

    fn decode_bencode_object(object: Object) -> Result<Self, Error>
    where
        Self: Sized,
    {
        let mut interval = None;
        let mut peers = None;

        let mut dict = object.try_into_dictionary()?;
        while let Some(pair) = dict.next_pair()? {
            match pair {
                (b"failure reason", value) => {
                    let failure = String::decode_bencode_object(value).map(Some)?;

                    return Ok(TrackerResponse {
                        failure,
                        success: None,
                    });
                }
                (b"interval", value) => {
                    let raw_interval = value.try_into_integer()?;

                    interval = u64::from_str(raw_interval)
                        .map(Some)
                        .map_err(Error::malformed_content)?;
                }
                (b"peers", value) => {
                    peers = match value {
                        Object::List(mut peers_list) => {
                            let mut tmp_peers = vec![];

                            while let Some(peer_obj) = peers_list.next_object()? {
                                tmp_peers.push(Peer::decode_bencode_object(peer_obj)?);
                            }

                            PeersFormat::Simple(tmp_peers)
                        }
                        Object::Bytes(peers_list) => PeersFormat::Compact(Vec::from(peers_list)),
                        unknown => {
                            return Err(Error::unexpected_token(
                                "Unknown",
                                unknown.into_token().name(),
                            ))
                        }
                    }
                    .into();
                }
                _ => (),
            }
        }

        let success = match (interval, peers) {
            (Some(interval), Some(peers)) => Some(Success { interval, peers }),
            _ => return Err(Error::missing_field("interval and/or peers")),
        };

        Ok(TrackerResponse {
            failure: None,
            success,
        })
    }
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
pub struct Interval(pub Duration);

impl Deref for Interval {
    type Target = Duration;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<u64> for Interval {
    fn from(raw: u64) -> Self {
        Interval(Duration::from_secs(raw))
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct PeerAddr(pub SocketAddr);

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
    type Error = TrackerError;

    fn try_from(peer: Peer) -> Result<Self, Self::Error> {
        let port = peer.port;
        let ip = match Host::parse(&peer.ip)
            .map_err(|_| TrackerError::PeersParserFailed("invalid peer ip"))?
        {
            Host::Domain(domain) => (domain, 0)
                .to_socket_addrs()
                .map_err(|_| TrackerError::PeersParserFailed("domain lookup failed"))?
                .next()
                .map(|saddr| saddr.ip())
                .ok_or(TrackerError::PeersParserFailed("unknown domain ip"))?,
            Host::Ipv4(ip) => IpAddr::V4(ip),
            Host::Ipv6(ip) => IpAddr::V6(ip),
        };

        Ok(SocketAddr::new(ip, port).into())
    }
}

impl TryFrom<&[u8]> for PeerAddr {
    type Error = TrackerError;

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
    type Error = TrackerError;

    fn try_from(response: TrackerResponse) -> Result<Self, Self::Error> {
        match (response.failure, response.success) {
            (Some(error), _) => Err(TrackerError::TrackerErrorResponse(error)),
            (None, Some(msg)) => {
                let interval = msg.interval.into();

                let addresses = match msg.peers {
                    PeersFormat::Simple(ready) => ready
                        .into_iter()
                        .map(|peer| peer.try_into())
                        .collect::<Result<_, TrackerError>>()?,
                    PeersFormat::Compact(raw) => {
                        const CPEER_SZ: usize = 6;

                        if raw.len() % CPEER_SZ != 0 {
                            return Err(TrackerError::PeersParserFailed(
                                "invalid peers compact format",
                            ));
                        }

                        (0..raw.len())
                            .step_by(CPEER_SZ)
                            .map(|i| (&raw[i..i + CPEER_SZ]).try_into())
                            .collect::<Result<_, TrackerError>>()?
                    }
                };

                Ok(Peers::new(interval, addresses))
            }
            _ => Err(TrackerError::PeersParserFailed("peers are missing")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TrackerClient {
    http_client: reqwest::Client,
    base_url: Arc<Url>,
}

impl TrackerClient {
    const FAKE_PORT: u16 = 6881;

    pub fn new(
        mut base_url: Url,
        info_hash: &InfoHash,
        peer_id: &PeerId,
        timeout: Duration,
    ) -> Self {
        let http_client = reqwest::Client::builder().timeout(timeout).build().unwrap();

        base_url
            .query_pairs_mut()
            .append_pair("info_hash", bytes_to_str(&info_hash[..]))
            .append_pair("peer_id", bytes_to_str(peer_id))
            .append_pair("compact", "1");

        let base_url = Arc::new(base_url);

        Self {
            http_client,
            base_url,
        }
    }

    pub async fn fetch_peers(
        &self,
        uploaded: usize,
        downloaded: usize,
        left: usize,
        event: Event,
    ) -> Result<Peers, TrackerError> {
        let body = self
            .http_client
            .get(self.base_url.as_str())
            .query(&[("port", Self::FAKE_PORT.to_string())])
            .query(&[("uploaded", uploaded)])
            .query(&[("downloaded", downloaded)])
            .query(&[("left", left)])
            .query(&[("event", event.as_str())])
            .send()
            .await
            .map_err(TrackerError::RequestFailed)?
            .error_for_status()
            .map_err(TrackerError::RequestFailed)?
            .bytes()
            .await
            .map_err(TrackerError::RequestFailed)?;

        TrackerResponse::from_bencode(&body)
            .map_err(|_| TrackerError::InvalidTrackerResponse)?
            .try_into()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
        time::Duration,
    };

    use bendy::decoding::FromBencode;
    use claims::{assert_matches, assert_none, assert_ok, assert_some, assert_some_eq};
    use url::Url;
    use wiremock::{
        matchers::{any, query_param, query_param_contains},
        Mock, MockServer, ResponseTemplate,
    };

    use crate::{InfoHash, PeerAddr, PeerId, TrackerClient};

    use super::*;

    #[test]
    fn parse_failure_response() {
        let raw = b"d14:failure reason4:teste";

        let response = assert_ok!(TrackerResponse::from_bencode(raw));
        assert_some_eq!(response.failure, "test");
        assert_none!(response.success);
    }

    #[test]
    fn parse_success_response_with_peers_in_simple_format() {
        let raw = b"d8:intervali6e5:peersld2:ip11:192.168.4.3\
            4:porti123eed2:ip12:192.232.4.324:porti545eeee";

        let response = assert_ok!(TrackerResponse::from_bencode(raw));
        assert_none!(response.failure);
        let success = assert_some!(response.success);
        assert_eq!(success.interval, 6);
        match success.peers {
            PeersFormat::Simple(peers) => {
                assert_eq!(peers.len(), 2);
                let peer1 = &peers[0];
                assert_eq!(peer1.ip, "192.168.4.3");
                assert_eq!(peer1.port, 123);
                let peer2 = &peers[1];
                assert_eq!(peer2.ip, "192.232.4.32");
                assert_eq!(peer2.port, 545);
            }
            _ => panic!("unexpecting peers compact format"),
        }
    }

    #[test]
    fn parse_success_response_with_peers_in_compact_format() {
        let raw = b"d8:intervali16e5:peers12:\xC0\xA8\x04\x03\x00\x7B\xC0\xE8\x04\x20\x02\x21e";

        let response = assert_ok!(TrackerResponse::from_bencode(raw));
        assert_none!(response.failure);
        let success = assert_some!(response.success);
        assert_eq!(success.interval, 16);
        match success.peers {
            PeersFormat::Compact(peers) => {
                assert_eq!(peers, b"\xC0\xA8\x04\x03\x00\x7B\xC0\xE8\x04\x20\x02\x21");
            }
            _ => panic!("unexpecting peers simple format"),
        }
    }

    #[test]
    fn parse_valid_interval() {
        let interval: Interval = 1_u64.into();

        assert_eq!(*interval, Duration::from_secs(1));
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
            Err(TrackerError::PeersParserFailed("invalid peer ip"))
        );

        assert_matches!(
            <Peer as TryInto<PeerAddr>>::try_into(Peer {
                ip: "[0:0:0:00:ffff:c00a:2ff]".into(),
                port: 45,
            }),
            Err(TrackerError::PeersParserFailed("invalid peer ip"))
        );

        assert_matches!(
            <Peer as TryInto<PeerAddr>>::try_into(Peer {
                ip: "localhost.invalid".into(),
                port: 45,
            }),
            Err(TrackerError::PeersParserFailed("domain lookup failed"))
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
                peers: PeersFormat::Compact(Vec::from(
                    &[192, 145, 21, 34, 0, 45, 54, 225, 23, 1, 17, 82][..],
                )),
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
        let _error = TrackerError::TrackerErrorResponse("test".into());

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
            Err(TrackerError::PeersParserFailed("peers are missing"))
        );
    }

    #[test]
    fn convert_invalid_tracker_response_with_invalid_peers_compat_format() {
        assert_matches!(
            <TrackerResponse as TryInto<Peers>>::try_into(TrackerResponse {
                failure: None,
                success: Some(Success {
                    interval: 2,
                    peers: PeersFormat::Compact(Vec::from(&[192, 145, 21, 34, 0][..])),
                }),
            }),
            Err(TrackerError::PeersParserFailed(
                "invalid peers compact format"
            ))
        );
    }

    #[tokio::test]
    async fn check_peers_request() {
        let mock_server = MockServer::start().await;

        let base_url = Url::parse(mock_server.uri().as_str()).unwrap();
        let peer_id = PeerId([
            0x2D, 0x52, 0x42, 0x30, 0x31, 0x30, 0x30, 0x2D, 0x98, 0x9D, 0xB1, 0x22, 0x2C, 0xCA,
            0x8C, 0xC5, 0x99, 0xC3, 0x42, 0xD4,
        ]);
        let timeout = Duration::from_millis(4000);
        let info_hash = InfoHash([
            0x8F, 0xFE, 0xAE, 0x56, 0xC3, 0x2A, 0xB5, 0x4D, 0x99, 0x92, 0xE4, 0xCB, 0xB2, 0xE,
            0xF0, 0x70, 0x63, 0x7A, 0x9C, 0x72,
        ]);

        Mock::given(any())
            .and(query_param_contains("info_hash", ""))
            .and(query_param_contains("peer_id", "-RB0100-"))
            .and(query_param("port", TrackerClient::FAKE_PORT.to_string()))
            .and(query_param("uploaded", "0"))
            .and(query_param("downloaded", "0"))
            .and(query_param("left", "658505728"))
            .and(query_param("compact", "1"))
            .and(query_param("event", "started"))
            .respond_with(
                ResponseTemplate::new(200).set_body_bytes(
                    b"d8:intervali16e5:peers12:\xC0\xA8\x04\x03\x00\x7B\xC0\xE8\x04\x20\x02\x21e"
                        .as_slice(),
                ),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = TrackerClient::new(base_url, &info_hash, &peer_id, timeout)
            .fetch_peers(0, 0, 658505728, super::Event::Started)
            .await;

        let response = assert_ok!(result);
        assert_eq!(response.interval.0, Duration::from_secs(16));
        assert_eq!(response.addresses.len(), 2);
        let peer1 = &response.addresses[0];
        assert_eq!(
            peer1.0,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 4, 3)), 123)
        );
        let peer2 = &response.addresses[1];
        assert_eq!(
            peer2.0,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 232, 4, 32)), 545)
        );
    }
}
