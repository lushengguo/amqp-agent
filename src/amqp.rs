use amqprs::{
    channel::{
        BasicPublishArguments, Channel, ConfirmSelectArguments, ExchangeDeclareArguments,
        ExchangeType,
    },
    connection::{Connection, OpenConnectionArguments},
    error::Error as AmqpError,
    BasicProperties,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::{self, Instant};
use tracing::{error, info, warn};
use url::Url;

use crate::config;
use crate::db::DB;
use crate::memory_cache::MemoryCache;
use crate::models::Message;
use std::error::Error;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{future::Future, pin::Pin};

const MAX_CONNECT_RETRIES: u32 = 3;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

lazy_static::lazy_static! {
    pub static ref CONNECTION_MANAGER: Arc<Mutex<AmqpConnectionManager>> = {
        let settings = config::Settings::new().expect("Failed to load config");
        let cache_size = config::parse_size(&settings.cache.max_size)
            .expect("Failed to parse cache size");
        let db = Arc::new(StdMutex::new(DB::new().expect("Failed to create DB")));
        let cache = Arc::new(StdMutex::new(MemoryCache::new(cache_size, db.clone())));
        Arc::new(Mutex::new(AmqpConnectionManager::new(db, cache)))
    };
}

#[derive(Debug, Clone)]
struct MessageStats {
    url: String,
    exchange: String,
    routing_key: String,
    total_sent: u64,
    total_success: u64,
    last_updated: Instant,
}

impl MessageStats {
    fn new(url: String, exchange: String, routing_key: String) -> Self {
        Self {
            url,
            exchange,
            routing_key,
            total_sent: 0,
            total_success: 0,
            last_updated: Instant::now(),
        }
    }
}

pub struct AmqpProduceer {
    url: String,
    connection: Option<Connection>,
    channel: Option<Channel>,
    last_heartbeat: Option<Instant>,
    cache: Arc<StdMutex<MemoryCache>>,
    declared_exchanges: HashMap<String, ExchangeType>,
    message_stats: HashMap<String, MessageStats>,
}

impl AmqpProduceer {
    pub fn new(url: String, cache: Arc<StdMutex<MemoryCache>>) -> Self {
        Self {
            url,
            connection: None,
            channel: None,
            last_heartbeat: None,
            cache,
            declared_exchanges: HashMap::new(),
            message_stats: HashMap::new(),
        }
    }

    fn parse_amqp_url(url_str: &str) -> Result<(String, String, String, u16)> {
        let url = Url::parse(url_str).map_err(|e| {
            Box::new(AmqpError::ChannelUseError(format!(
                "Invalid AMQP URL: {}",
                e
            ))) as Box<dyn Error + Send + Sync>
        })?;

        if url.scheme() != "amqp" {
            return Err(Box::new(AmqpError::ChannelUseError(
                "URL scheme must be 'amqp'".to_string(),
            )) as Box<dyn Error + Send + Sync>);
        }

        let host = url.host_str().unwrap_or("localhost").to_string();
        let port = url.port().unwrap_or(5672);
        let username = url.username().to_string();
        let password = url.password().unwrap_or("").to_string();

        Ok((host, username, password, port))
    }

    async fn do_connect(&mut self) -> Result<()> {
        let (host, username, password, port) = Self::parse_amqp_url(&self.url)?;
        info!(
            "Connecting to RabbitMQ server - Host: {}, Port: {}, Username: {}",
            host, port, username
        );
        let args = OpenConnectionArguments::new(&host, port, &username, &password);

        match Connection::open(&args).await {
            Ok(connection) => {
                info!("Successfully established connection to RabbitMQ");
                self.connection = Some(connection);
                let channel = self.connection.as_ref().unwrap().open_channel(None).await?;
                info!("Successfully opened RabbitMQ channel");
                self.channel = Some(channel);
                self.channel
                    .as_ref()
                    .unwrap()
                    .confirm_select(ConfirmSelectArguments::default())
                    .await?;
                info!("Message confirmation mode enabled");
                self.last_heartbeat = Some(Instant::now());
                Ok(())
            }
            Err(e) => {
                error!(
                    "Failed to connect to RabbitMQ - Host: {}, Port: {}, Username: {}, Error: {}",
                    host, port, username, e
                );
                Err(Box::new(e) as Box<dyn Error + Send + Sync>)
            }
        }
    }

    fn clear_connection_state(&mut self) {
        self.connection = None;
        self.channel = None;
        self.last_heartbeat = None;
        self.declared_exchanges.clear();
    }

    fn connect(&mut self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            if self.connection.is_some() {
                return Ok(());
            }

            let mut retries = 0;
            while retries < MAX_CONNECT_RETRIES {
                match self.do_connect().await {
                    Ok(_) => return Ok(()),
                    Err(e) => {
                        let error_msg = format!("Failed to connect to RabbitMQ: {}", e);
                        warn!("{}, will retry in 3 seconds", error_msg);
                        time::sleep(Duration::from_secs(3)).await;
                        retries += 1;
                    }
                }
            }

            Err(Box::new(AmqpError::ChannelUseError(
                "Unable to establish connection".to_string(),
            )) as Box<dyn Error + Send + Sync>)
        })
    }

    async fn declare_exchange(
        &mut self,
        exchange: &str,
        exchange_type: &ExchangeType,
    ) -> Result<()> {
        if let Some(cached_type) = self.declared_exchanges.get(exchange) {
            if cached_type == exchange_type {
                return Ok(());
            }
        }

        if let Some(channel) = &self.channel {
            info!(
                "Declaring Exchange - Name: {}, Type: {:?}",
                exchange, exchange_type
            );

            let exchange_type_str = exchange_type.to_string();
            let args: &mut ExchangeDeclareArguments =
                &mut ExchangeDeclareArguments::new(exchange, &exchange_type_str);

            args.durable(true).auto_delete(false);

            channel.exchange_declare(args.clone()).await?;
            self.declared_exchanges.insert(
                exchange.to_string(),
                ExchangeType::from(exchange_type.to_string()),
            );
            info!("Exchange declared successfully - {}", exchange);
            Ok(())
        } else {
            Err(Box::new(AmqpError::ChannelUseError(
                "Channel not initialized".to_string(),
            )) as Box<dyn Error + Send + Sync>)
        }
    }

    pub async fn produce(
        &mut self,
        exchange: &str,
        exchange_type: &str,
        routing_key: &str,
        message: &[u8],
        is_retry: bool,
    ) -> Result<()> {
        // Update message stats
        let stats_key = Self::get_stats_key(exchange, routing_key);
        let stats = self
            .message_stats
            .entry(stats_key.clone())
            .or_insert_with(|| {
                MessageStats::new(
                    self.url.clone(),
                    exchange.to_string(),
                    routing_key.to_string(),
                )
            });
        stats.total_sent += 1;
        stats.last_updated = Instant::now();

        if !self.is_connected() {
            self.connect().await?;
        }

        let exchange_type = match exchange_type.to_lowercase().as_str() {
            "direct" => ExchangeType::Direct,
            "fanout" => ExchangeType::Fanout,
            "topic" => ExchangeType::Topic,
            "headers" => ExchangeType::Headers,
            _ => {
                warn!(
                    "Unknown Exchange type: {}, using default type Topic",
                    exchange_type
                );
                ExchangeType::Topic
            }
        };

        let exchange_type_str = exchange_type.to_string();
        if let Err(e) = self.declare_exchange(exchange, &exchange_type).await {
            warn!(
                "Failed to declare Exchange: {} - attempting to send message anyway",
                e
            );
        }

        let log_prefix = if is_retry { "[RETRY]" } else { "" };
        info!(
            "{}Preparing to send message - Exchange: {} ({:?}), RoutingKey: {}, Message size: {} bytes",
            log_prefix,
            exchange,
            exchange_type_str,
            routing_key,
            message.len()
        );

        let args = BasicPublishArguments::new(exchange, routing_key);
        match self
            .channel
            .as_ref()
            .unwrap()
            .basic_publish(BasicProperties::default(), message.to_vec(), args)
            .await
        {
            Ok(_) => {
                info!(
                    "{}Message sent successfully - Exchange: {} ({:?}), RoutingKey: {}, Message size: {} bytes",
                    log_prefix,
                    exchange,
                    exchange_type_str,
                    routing_key,
                    message.len()
                );
                self.last_heartbeat = Some(Instant::now());
                // Update success count in stats
                if let Some(stats) = self.message_stats.get_mut(&stats_key) {
                    stats.total_success += 1;
                }
                Ok(())
            }
            Err(e) => {
                let error_msg = format!(
                    "{}Failed to send message - Exchange: {} ({:?}), RoutingKey: {}, Message size: {} bytes, Error: {}",
                    log_prefix,
                    exchange,
                    exchange_type_str,
                    routing_key,
                    message.len(),
                    e
                );
                error!("{}", error_msg);
                if !is_retry {
                    self.cache_message(exchange, exchange_type_str, routing_key, message)
                        .await?;
                }
                Err(Box::new(AmqpError::ChannelUseError(error_msg)) as Box<dyn Error + Send + Sync>)
            }
        }
    }

    async fn cache_message(
        &self,
        exchange: &str,
        exchange_type: String,
        routing_key: &str,
        message: &[u8],
    ) -> Result<()> {
        let message_str = String::from_utf8_lossy(message).to_string();
        let exchange_type_clone = exchange_type.clone();
        info!(
            "Caching message - Exchange: {} ({}), RoutingKey: {}, Message size: {} bytes",
            exchange,
            exchange_type_clone,
            routing_key,
            message.len()
        );

        let message = Message {
            url: self.url.clone(),
            exchange: exchange.to_string(),
            exchange_type,
            routing_key: routing_key.to_string(),
            message: message_str,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as u32,
        };

        let mut cache = self.cache.lock().unwrap();
        cache.insert(message);
        info!(
            "Message cached successfully - Exchange: {} ({}), RoutingKey: {}, Current memory usage: {} bytes, cached {} messages",
            exchange,
            exchange_type_clone,
            routing_key,
            cache.memory_usage(),
            cache.message_count()
        );
        Ok(())
    }

    pub fn is_connected(&mut self) -> bool {
        if let Some(last_heartbeat) = self.last_heartbeat {
            if last_heartbeat.elapsed() > Duration::from_secs(30) {
                self.clear_connection_state();
                return false;
            }
            true
        } else {
            false
        }
    }

    pub async fn retry_message_count(&mut self) -> Result<()> {
        if !self.is_connected() {
            return Ok(());
        }

        let messages = {
            let cache = self.cache.lock().unwrap();
            cache.get_recent(1000)
        };

        for message in messages {
            match self
                .produce(
                    &message.exchange,
                    &message.exchange_type,
                    &message.routing_key,
                    message.message.as_bytes(),
                    true,
                )
                .await
            {
                Ok(_) => {
                    let mut cache = self.cache.lock().unwrap();
                    cache.remove(message.timestamp);
                    info!(
                        "[RETRY] Successfully removed message from cache - Exchange: {}, RoutingKey: {}",
                        message.exchange,
                        message.routing_key
                    );
                }
                Err(e) => {
                    error!("[RETRY] Failed to retry message: {}", e);
                }
            }
        }

        Ok(())
    }

    pub async fn start_retry_task(producer: Arc<Mutex<AmqpProduceer>>) {
        tokio::spawn(async move {
            loop {
                time::sleep(Duration::from_secs(5)).await;
                let mut producer = producer.lock().await;
                if let Err(e) = producer.retry_message_count().await {
                    error!("Failed to retry cached messages: {}", e);
                }
            }
        });
    }

    pub async fn start_heartbeat_task(producer: Arc<Mutex<AmqpProduceer>>) {
        tokio::spawn(async move {
            loop {
                time::sleep(Duration::from_secs(15)).await;
                let mut producer = producer.lock().await;
                if !producer.is_connected() {
                    warn!("Heartbeat check failed, reconnecting to {}", producer.url);
                    if let Err(e) = producer.connect().await {
                        error!("Failed to reconnect: {}", e);
                    } else {
                        info!("Reconnected to {}", producer.url);
                    }
                }
            }
        });
    }

    fn get_stats_key(exchange: &str, routing_key: &str) -> String {
        format!("{}:{}", exchange, routing_key)
    }

    pub fn generate_report(&self) -> Vec<PeriodProduceReport> {
        let now = Instant::now();
        let mut report = Vec::new();
        for stats in self.message_stats.values() {
            if now.duration_since(stats.last_updated) < Duration::from_secs(60) {
                report.push(PeriodProduceReport {
                    url: stats.url.clone(),
                    exchange: stats.exchange.clone(),
                    routing_key: stats.routing_key.clone(),
                    total_sent: stats.total_sent,
                    total_success: stats.total_success,
                });
            }
        }
        report
    }
}

pub struct AmqpConnectionManager {
    producers: HashMap<String, Arc<Mutex<AmqpProduceer>>>,
    db: Arc<StdMutex<DB>>,
    cache: Arc<StdMutex<MemoryCache>>,
}

pub struct PeriodProduceReport {
    pub url: String,
    pub exchange: String,
    pub routing_key: String,
    pub total_sent: u64,
    pub total_success: u64,
}

pub struct CacheReport {
    pub in_memory_message_count: u64,
    pub memory_usage: u64,
    pub in_disk_message_count: u64,
    pub disk_usage: u64,
}

impl AmqpConnectionManager {
    pub fn new(db: Arc<StdMutex<DB>>, cache: Arc<StdMutex<MemoryCache>>) -> Self {
        Self {
            producers: HashMap::new(),
            db,
            cache,
        }
    }

    pub fn get_db(&self) -> Arc<StdMutex<DB>> {
        self.db.clone()
    }

    pub async fn get_or_create_producer(
        &mut self,
        url: String,
    ) -> Result<Arc<Mutex<AmqpProduceer>>> {
        let (host, username, password, port) = AmqpProduceer::parse_amqp_url(&url)?;
        if host.is_empty() || username.is_empty() || password.is_empty() || port == 0 {
            return Err(Box::new(AmqpError::ChannelUseError(format!(
                "Invalid AMQP URL {}",
                url
            ))) as Box<dyn Error + Send + Sync>);
        }

        if let Some(producer) = self.producers.get(&url) {
            return Ok(producer.clone());
        }

        let producer = Arc::new(Mutex::new(AmqpProduceer::new(
            url.clone(),
            self.cache.clone(),
        )));

        AmqpProduceer::start_heartbeat_task(producer.clone()).await;
        AmqpProduceer::start_retry_task(producer.clone()).await;

        self.producers.insert(url, producer.clone());
        Ok(producer)
    }

    pub async fn generate_reports(&self) -> (Vec<PeriodProduceReport>, CacheReport) {
        let mut produce_report = Vec::new();

        for producer in self.producers.values() {
            let producer_guard = producer.lock().await;
            produce_report.extend(producer_guard.generate_report());
        }

        (
            produce_report,
            CacheReport {
                in_memory_message_count: self.cache.lock().unwrap().message_count() as u64,
                memory_usage: self.cache.lock().unwrap().memory_usage() as u64,
                in_disk_message_count: self.db.lock().unwrap().message_count().unwrap() as u64,
                disk_usage: self.db.lock().unwrap().disk_usage().unwrap() as u64,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_RABBITMQ_URL: &str = "amqp://guest:guest@localhost:5672";

    #[tokio::test]
    async fn test_connect_to_local_rabbitmq() {
        let db = Arc::new(StdMutex::new(DB::new().unwrap()));
        let mut producer = AmqpProduceer::new(
            TEST_RABBITMQ_URL.to_string(),
            Arc::new(StdMutex::new(MemoryCache::new(1000, db.clone()))),
        );

        match producer.connect().await {
            Ok(_) => assert!(true),
            Err(e) => {
                println!("Failed to connect to local RabbitMQ: {}", e);
                assert!(false);
            }
        }
    }

    #[tokio::test]
    async fn test_produce_with_different_exchange_types() {
        let db = Arc::new(StdMutex::new(DB::new().unwrap()));
        let mut producer = AmqpProduceer::new(
            TEST_RABBITMQ_URL.to_string(),
            Arc::new(StdMutex::new(MemoryCache::new(1000, db.clone()))),
        );

        let test_cases = vec![
            (
                "test_direct",
                "direct",
                "direct_key",
                "Direct exchange test",
            ),
            ("test_fanout", "fanout", "", "Fanout exchange test"),
            (
                "test_topic",
                "topic",
                "topic.test.key",
                "Topic exchange test",
            ),
        ];

        for (exchange, ex_type, routing_key, message) in test_cases {
            match producer
                .produce(exchange, ex_type, routing_key, message.as_bytes(), false)
                .await
            {
                Ok(_) => {
                    println!("Successfully sent to {} ({}) exchange", exchange, ex_type);
                    assert!(true);
                }
                Err(e) => {
                    println!(
                        "Failed to send to {} ({}) exchange: {}",
                        exchange, ex_type, e
                    );
                    assert!(false);
                }
            }
        }
    }

    #[tokio::test]
    async fn test_connect_to_invalid_host() {
        let db = Arc::new(StdMutex::new(DB::new().unwrap()));
        let mut producer = AmqpProduceer::new(
            "amqp://guest:guest@non_existent_host:5672".to_string(),
            Arc::new(StdMutex::new(MemoryCache::new(1000, db.clone()))),
        );

        match producer.connect().await {
            Ok(_) => {
                assert!(false, "Should not be able to connect to invalid host");
            }
            Err(_) => assert!(true),
        }
    }

    #[tokio::test]
    async fn test_produce_with_cache() {
        let db = Arc::new(StdMutex::new(DB::new().unwrap()));
        let cache = Arc::new(StdMutex::new(MemoryCache::new(1000, db.clone())));
        let mut producer = AmqpProduceer::new(
            "amqp://guest:guest@non_existent_host:5672".to_string(),
            cache.clone(),
        );

        let test_message = Message {
            url: "amqp://guest:guest@non_existent_host:5672".to_string(),
            exchange: "test_exchange".to_string(),
            exchange_type: "topic".to_string(),
            routing_key: "test.key".to_string(),
            message: "Test message".to_string(),
            timestamp: 12345,
        };

        {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.insert(test_message.clone());
        }

        match producer.connect().await {
            Ok(_) => assert!(false, "Should not connect to invalid host"),
            Err(_) => {
                let cache_guard = cache.lock().unwrap();
                let messages = cache_guard.get_recent(10);
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].exchange, "test_exchange");
                assert_eq!(messages[0].routing_key, "test.key");
            }
        }
    }

    #[tokio::test]
    async fn test_message_stats() {
        let db = Arc::new(StdMutex::new(DB::new().unwrap()));
        let mut producer = AmqpProduceer::new(
            TEST_RABBITMQ_URL.to_string(),
            Arc::new(StdMutex::new(MemoryCache::new(1000, db.clone()))),
        );

        // Send a test message
        let result = producer
            .produce(
                "test_exchange",
                "topic",
                "test.key",
                "Test message".as_bytes(),
                false,
            )
            .await;

        // Get the report
        let report = producer.generate_report();

        // Check if stats were recorded
        assert!(!report.is_empty());
        let PeriodProduceReport {
            url,
            exchange,
            routing_key,
            total_sent,
            total_success,
        } = &report[0];
        assert_eq!(url, TEST_RABBITMQ_URL);
        assert_eq!(exchange, "test_exchange");
        assert_eq!(routing_key, "test.key");
        assert_eq!(*total_sent, 1);
        assert_eq!(*total_success, result.is_ok() as u64);
    }
}
