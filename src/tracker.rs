#[derive(Debug, serde::Deserialize)]
pub struct Peer {
    pub id: String,
    pub ip: String,
    pub port: i64,
}

#[derive(Debug, serde::Deserialize)]
pub struct Success {
    pub interval: i64,
    pub peers: Vec<Peer>,
}

#[derive(Debug, serde::Deserialize)]
pub struct Response {
    pub failure: Option<String>,
    #[serde(flatten)]
    pub success: Option<Success>,
}
