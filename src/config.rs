use config::Config;
use serde::Deserialize;
use std::error::Error;

#[derive(Debug, Deserialize, Clone)]
pub struct LogConfig {
    pub level: String,
    pub directory: String,
    pub prefix: String,
    pub max_days: u64,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub server: ServerConfig,
    pub log: LogConfig,
}

impl Settings {
    pub fn new() -> Result<Self, Box<dyn Error>> {
        let settings = Config::builder()
            .add_source(config::File::with_name("config/default"))
            .build()?
            .try_deserialize::<Settings>()?;
        Ok(settings)
    }
}
