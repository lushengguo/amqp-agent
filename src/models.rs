#[derive(Debug, serde::Deserialize, Clone)]
pub struct Message {
    pub url: String,
    pub exchange: String,
    pub routing_key: String,
    pub message: String,
    pub timestamp: u32,
}
