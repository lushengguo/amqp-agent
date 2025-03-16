use md5;

#[derive(Debug, serde::Deserialize, Clone)]
pub struct Message {
    pub url: String,
    pub exchange: String,
    pub exchange_type: String,
    pub routing_key: String,
    pub message: String,
    pub timestamp: u32,
}

impl Message {
pub fn locator(&self) ->String{
    format!(
        "{}.{}.{}.{}.{:?}",
        self.url, self.exchange, self.exchange_type, self.routing_key, md5::compute(self.message.clone())
    )
}
}
