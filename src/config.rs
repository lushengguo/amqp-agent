use config::Config;
use serde::Deserialize;
use std::error::Error;
use std::time::Duration;

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub server: ServerSettings,
    pub log: LogSettings,
    pub cache: CacheSettings,
}

#[derive(Debug, Deserialize)]
pub struct ServerSettings {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LogSettings {
    pub dir: String,
    pub level: String,
    pub max_size: String,
    pub max_files: u32,
    pub clean_interval: String,
}

#[derive(Debug, Deserialize)]
pub struct CacheSettings {
    pub max_size: String,
}

#[derive(Debug)]
pub enum ConfigParseError {
    InvalidNumber(String),
    UnsupportedUnit(String),
}

impl std::fmt::Display for ConfigParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigParseError::InvalidNumber(msg) => write!(f, "Invalid number: {}", msg),
            ConfigParseError::UnsupportedUnit(msg) => write!(f, "Unsupported unit: {}", msg),
        }
    }
}

impl std::error::Error for ConfigParseError {}

impl Settings {
    pub fn new() -> std::result::Result<Self, Box<dyn Error + Send + Sync>> {
        let settings = Config::builder()
            .add_source(config::File::with_name("config/default"))
            .build()
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?
            .try_deserialize::<Settings>()
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;
        Ok(settings)
    }
}

pub fn parse_size(size_str: &str) -> std::result::Result<usize, Box<dyn Error + Send + Sync>> {
    let size_str = size_str.trim().to_uppercase();
    let mut num_str = String::new();
    let mut unit = String::new();

    for c in size_str.chars() {
        if c.is_digit(10) {
            num_str.push(c);
        } else {
            unit.push(c);
        }
    }

    let base: usize = num_str.parse().map_err(|_| {
        Box::new(ConfigParseError::InvalidNumber(num_str.clone())) as Box<dyn Error + Send + Sync>
    })?;

    match unit.trim() {
        "KB" => Ok(base * 1024),
        "MB" => Ok(base * 1024 * 1024),
        "GB" => Ok(base * 1024 * 1024 * 1024),
        "" => Ok(base),
        _ => Err(Box::new(ConfigParseError::UnsupportedUnit(unit)) as Box<dyn Error + Send + Sync>),
    }
}

pub fn parse_duration(
    duration_str: &str,
) -> std::result::Result<Duration, Box<dyn Error + Send + Sync>> {
    let duration_str = duration_str.trim().to_uppercase();
    let mut num_str = String::new();
    let mut unit = String::new();

    for c in duration_str.chars() {
        if c.is_digit(10) {
            num_str.push(c);
        } else {
            unit.push(c);
        }
    }

    let base: u64 = num_str.parse().map_err(|_| {
        Box::new(ConfigParseError::InvalidNumber(num_str.clone())) as Box<dyn Error + Send + Sync>
    })?;

    match unit.trim() {
        "S" => Ok(Duration::from_secs(base)),
        "M" => Ok(Duration::from_secs(base * 60)),
        "H" => Ok(Duration::from_secs(base * 3600)),
        "D" => Ok(Duration::from_secs(base * 86400)),
        "" => Ok(Duration::from_secs(base)),
        _ => Err(Box::new(ConfigParseError::UnsupportedUnit(unit)) as Box<dyn Error + Send + Sync>),
    }
}
