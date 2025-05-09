use config::Config;
use serde::Deserialize;
use std::error::Error;

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
    pub max_kept_files: u32,
}

#[derive(Debug, Deserialize)]
pub struct CacheSettings {
    pub max_size: String,
    pub enable_sqlite: bool,
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
    pub fn new(yaml_file_path: &str) -> std::result::Result<Self, Box<dyn Error + Send + Sync>> {
        let settings = Config::builder()
            .add_source(
                config::File::with_name(yaml_file_path)
                    .required(true)
                    .format(config::FileFormat::Yaml),
            )
            .build()
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?
            .try_deserialize::<Settings>()
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;
        Ok(settings)
    }

    pub fn from_string(content: &str) -> std::result::Result<Self, Box<dyn Error + Send + Sync>> {
        let settings: Settings = serde_yaml::from_str(content)
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

#[test]
fn test_parse_yaml_content() {
    let yaml_content = r#"
    server:
      port: 8080
      host: "127.0.0.1"
    cache:
      max_size: "100MB"
      enable_sqlite: false
    log:
      level: "debug"
      dir: "logs"
      max_kept_files: 5
    "#;

    let settings = Settings::from_string(yaml_content).expect("Failed to parse YAML");
    assert_eq!(settings.server.port, 8080);
    assert_eq!(settings.server.host, "127.0.0.1");
    assert_eq!(settings.cache.max_size, "100MB");
    assert_eq!(settings.cache.enable_sqlite, false);
    assert_eq!(settings.log.level, "debug");
    assert_eq!(settings.log.dir, "logs");
    assert_eq!(settings.log.max_kept_files, 5);
}

#[test]
fn test_parse_yaml_from_file() {
    let settings = Settings::new("./config/amqp_agent_test.yaml").expect("Failed to load settings from file");
    assert_eq!(settings.server.port, 8080);
    assert_eq!(settings.server.host, "127.0.0.1");
    assert_eq!(settings.cache.max_size, "100MB");
    assert_eq!(settings.cache.enable_sqlite, false);
    assert_eq!(settings.log.level, "debug");
    assert_eq!(settings.log.dir, "logs");
    assert_eq!(settings.log.max_kept_files, 5);
}