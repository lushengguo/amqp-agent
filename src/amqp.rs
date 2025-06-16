use crate::config;
use crate::db::DB;
use crate::memory_cache::MemoryCache;
use crate::models::Message;
use amqprs::{
    channel::{
        BasicPublishArguments, Channel, ConfirmSelectArguments, ExchangeDeclareArguments,
        ExchangeType,
    },
    connection::{Connection, OpenConnectionArguments},
    error::Error as AmqpError,
    BasicProperties,
};
use parking_lot::Mutex as ParkingLotMutex;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{future::Future, pin::Pin};
use tokio::sync::Mutex as TokioMutex;
use tokio::time::{self, Instant};
use url::Url;

const MAX_CONNECT_RETRIES: u32 = 3;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

lazy_static::lazy_static! {
    pub static ref CONNECTION_MANAGER: Arc<TokioMutex<AmqpConnectionManager>> = {
        let settings = config::Settings::new("./config/amqp_agent.yaml").expect("Failed to load config");
        let cache_size = config::parse_size(&settings.cache.max_size)
            .expect("Failed to parse cache size");
        let db = Arc::new(ParkingLotMutex::new(DB::new().expect("Failed to create DB")));
        let cache = Arc::new(ParkingLotMutex::new(MemoryCache::new(cache_size, db.clone(), settings.cache.enable_sqlite)));
        Arc::new(TokioMutex::new(AmqpConnectionManager::new(db, cache)))
    };
}

#[derive(Debug, Clone)]
struct MessageStatistic {
    url: String,
    exchange: String,
    routing_key: String,
    produced: u64,
    confirmed: u64,
    update_timestamp: Instant,
}

impl MessageStatistic {
    fn new(url: String, exchange: String, routing_key: String) -> Self {
        Self {
            url,
            exchange,
            routing_key,
            produced: 0,
            confirmed: 0,
            update_timestamp: Instant::now(),
        }
    }
}

pub struct AmqpProduceer {
    url: String,
    connection: Option<Connection>,
    channel: Option<Channel>,
    previous_confirm_timestamp: Option<Instant>,
    cache: Arc<ParkingLotMutex<MemoryCache>>,
    declared_exchanges: HashMap<String, ExchangeType>,
    message_statistic: HashMap<String, MessageStatistic>,
}

impl AmqpProduceer {
    pub fn new(url: String, cache: Arc<ParkingLotMutex<MemoryCache>>) -> Self {
        Self {
            url,
            connection: None,
            channel: None,
            previous_confirm_timestamp: None,
            cache,
            declared_exchanges: HashMap::new(),
            message_statistic: HashMap::new(),
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
log::info!(
            "Connecting to RabbitMQ server - Host: {}, Port: {}, Username: {}",
            host, port, username
        );
        let args = OpenConnectionArguments::new(&host, port, &username, &password);

        match Connection::open(&args).await {
            Ok(connection) => {
log::info!(
                    "Successfully established connection to RabbitMQ, Host: {}, Port: {}, Username: {}",
                    host, port, username
                );
                self.connection = Some(connection);
                let channel = self.connection.as_ref().unwrap().open_channel(None).await?;
log::info!("Successfully opened RabbitMQ channel");
                self.channel = Some(channel);
                self.channel
                    .as_ref()
                    .unwrap()
                    .confirm_select(ConfirmSelectArguments::default())
                    .await?;
log::info!("Message confirmation mode enabled");
                self.previous_confirm_timestamp = Some(Instant::now());
                Ok(())
            }
            Err(e) => {
log::error!(
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
        self.previous_confirm_timestamp = None;
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
log::warn!("{}, will retry in 3 seconds", error_msg);
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
log::info!(
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
log::info!("Exchange declared successfully - {}", exchange);
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
        let statistic_key = Self::get_statistic_key(exchange, routing_key);
        let statistic = self
            .message_statistic
            .entry(statistic_key.clone())
            .or_insert_with(|| {
                MessageStatistic::new(
                    self.url.clone(),
                    exchange.to_string(),
                    routing_key.to_string(),
                )
            });
        statistic.produced += 1;
        statistic.update_timestamp = Instant::now();

        if !self.is_connected() {
            self.connect().await?;
        }

        let exchange_type = match exchange_type.to_lowercase().as_str() {
            "direct" => ExchangeType::Direct,
            "fanout" => ExchangeType::Fanout,
            "topic" => ExchangeType::Topic,
            "headers" => ExchangeType::Headers,
            _ => {
log::warn!(
                    "Unknown Exchange type: {}, using default type Topic",
                    exchange_type
                );
                ExchangeType::Topic
            }
        };

        let exchange_type_str = exchange_type.to_string();
        if let Err(e) = self.declare_exchange(exchange, &exchange_type).await {
log::warn!(
                "Failed to declare Exchange: {} - attempting to send message anyway",
                e
            );
        }

        let log_prefix = if is_retry { "[RETRY]" } else { "" };
log::debug!(
            "{}Preparing to send message - Exchange: {} ({:?}), RoutingKey: {}, Message size: {} bytes, Message Body: {}",
            log_prefix,
            exchange,
            exchange_type_str,
            routing_key,
            message.len(), 
            String::from_utf8_lossy(message)
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
log::debug!(
                    "{}Message sent successfully - Exchange: {} ({:?}), RoutingKey: {}, Message size: {} bytes, Message Body: {}",
                    log_prefix,
                    exchange,
                    exchange_type_str,
                    routing_key,
                    message.len(),
                    String::from_utf8_lossy(message)
                );
                self.previous_confirm_timestamp = Some(Instant::now());

                if let Some(statistic) = self.message_statistic.get_mut(&statistic_key) {
                    statistic.confirmed += 1;
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
log::error!("{}", error_msg);
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
log::info!(
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

        let mut cache = self.cache.lock();
        let locator = message.locator();
        let exchange_clone = message.exchange.clone();
        let routing_key_clone = message.routing_key.clone();

        cache.insert(message);
log::info!(
            "Message cached successfully - Exchange: {} ({}), RoutingKey: {}, Current memory usage: {} bytes, cached {} messages",
            exchange,
            exchange_type_clone,
            routing_key,
            cache.memory_usage(),
            cache.message_count()
        );
log::info!(
            "Removing message from cache - Exchange: {}, RoutingKey: {}, Total messages in cache: {}",
            exchange_clone, routing_key_clone,
            cache.message_count()
        );
        cache.remove(&locator);
        Ok(())
    }

    pub fn is_connected(&mut self) -> bool {
        if let Some(previous_confirm_timestamp) = self.previous_confirm_timestamp {
            if previous_confirm_timestamp.elapsed() > Duration::from_secs(30) {
                self.clear_connection_state();
                return false;
            }
            true
        } else {
            false
        }
    }

    pub async fn retry_produce_cached_messages(&mut self) -> Result<()> {
        if !self.is_connected() {
            return Ok(());
        }

        let messages = {
            let cache = self.cache.lock();
            cache.get_recent_messages(1000)
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
                    let mut cache = self.cache.lock();
                    let locator = message.locator();
                    let exchange = message.exchange.clone();
                    let routing_key = message.routing_key.clone();
log::debug!(
                        "[RETRY] Successfully removed message from cache - Exchange: {}, RoutingKey: {}",
                        exchange, routing_key
                    );
                    cache.remove(&locator);
                }
                Err(e) => {
log::error!("[RETRY] Failed to retry message: {}", e);
                }
            }
        }

        Ok(())
    }

    pub async fn start_retry_task(producer: Arc<TokioMutex<AmqpProduceer>>) {
        tokio::spawn(async move {
            loop {
                time::sleep(Duration::from_secs(5)).await;
                let mut producer = producer.lock().await;
                if let Err(e) = producer.retry_produce_cached_messages().await {
log::error!("Failed to retry produce cached messages: {}", e);
                }
            }
        });
    }

    pub async fn start_heartbeat_task(producer: Arc<TokioMutex<AmqpProduceer>>) {
        tokio::spawn(async move {
            loop {
                time::sleep(Duration::from_secs(15)).await;
                let mut producer = producer.lock().await;
                if !producer.is_connected() {
log::warn!("Heartbeat check failed, reconnecting to {}", producer.url);
                    if let Err(e) = producer.connect().await {
log::error!("Failed to reconnect: {}", e);
                    } else {
log::info!("Reconnected to {}", producer.url);
                    }
                }
            }
        });
    }

    fn get_statistic_key(exchange: &str, routing_key: &str) -> String {
        format!("{}:{}", exchange, routing_key)
    }

    pub fn clear_report_cache(&mut self) {
        self.message_statistic.clear();
    }

    pub fn generate_report(&mut self, duration: Duration) -> Vec<PeriodProduceReport> {
        let now = Instant::now();
        let mut report = Vec::new();
        for statistic in self.message_statistic.values() {
            if now.duration_since(statistic.update_timestamp) < duration {
                report.push(PeriodProduceReport {
                    url: statistic.url.clone(),
                    exchange: statistic.exchange.clone(),
                    routing_key: statistic.routing_key.clone(),
                    produced: statistic.produced,
                    confirmed: statistic.confirmed,
                });
            }
        }

        for statistic in self.message_statistic.values_mut() {
            statistic.produced = 0;
            statistic.confirmed = 0;
        }
        report
    }
}

pub struct AmqpConnectionManager {
    producers: HashMap<String, Arc<TokioMutex<AmqpProduceer>>>,
    db: Arc<ParkingLotMutex<DB>>,
    cache: Arc<ParkingLotMutex<MemoryCache>>,
}

pub struct PeriodProduceReport {
    pub url: String,
    pub exchange: String,
    pub routing_key: String,
    pub produced: u64,
    pub confirmed: u64,
}

pub struct CacheReport {
    pub in_memory_message_count: u64,
    pub memory_usage: u64,
    pub in_disk_message_count: u64,
    pub disk_usage: u64,
}

impl AmqpConnectionManager {
    pub fn new(db: Arc<ParkingLotMutex<DB>>, cache: Arc<ParkingLotMutex<MemoryCache>>) -> Self {
        Self {
            producers: HashMap::new(),
            db,
            cache,
        }
    }

    pub async fn get_or_create_producer(
        &mut self,
        url: String,
    ) -> Result<Arc<TokioMutex<AmqpProduceer>>> {
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

        let producer = Arc::new(TokioMutex::new(AmqpProduceer::new(
            url.clone(),
            self.cache.clone(),
        )));

        AmqpProduceer::start_heartbeat_task(producer.clone()).await;
        AmqpProduceer::start_retry_task(producer.clone()).await;

        self.producers.insert(url, producer.clone());
        Ok(producer)
    }

    pub async fn generate_reports(
        &self,
        duration: Duration,
    ) -> (Vec<PeriodProduceReport>, CacheReport) {
        let mut produce_report = Vec::new();

        for producer in self.producers.values() {
            let mut producer_guard = producer.lock().await;
            produce_report.extend(producer_guard.generate_report(duration));
            producer_guard.clear_report_cache();
        }

        let cache = self.cache.lock();
        let in_memory_message_count = cache.message_count() as u64;
        let memory_usage = cache.memory_usage() as u64;
        drop(cache);

        let db = self.db.lock();
        let in_disk_message_count = db.message_count().unwrap() as u64;
        let disk_usage = db.disk_usage().unwrap() as u64;

        (
            produce_report,
            CacheReport {
                in_memory_message_count,
                memory_usage,
                in_disk_message_count,
                disk_usage,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_RABBITMQ_URL: &str = "amqp://guest:guest@localhost:5672";

    #[tokio::test]
    async fn test_connect_to_local_rabbitmq_rely_on_local_rabbitmq() {
        let db = Arc::new(ParkingLotMutex::new(DB::new().unwrap()));
        let mut producer = AmqpProduceer::new(
            TEST_RABBITMQ_URL.to_string(),
            Arc::new(ParkingLotMutex::new(MemoryCache::new(
                1000,
                db.clone(),
                true,
            ))),
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
    async fn test_produce_with_different_exchange_types_rely_on_local_rabbitmq() {
        let db = Arc::new(ParkingLotMutex::new(DB::new().unwrap()));
        let mut producer = AmqpProduceer::new(
            TEST_RABBITMQ_URL.to_string(),
            Arc::new(ParkingLotMutex::new(MemoryCache::new(
                1000,
                db.clone(),
                true,
            ))),
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
        let db = Arc::new(ParkingLotMutex::new(DB::new().unwrap()));
        let mut producer = AmqpProduceer::new(
            "amqp://guest:guest@non_existent_host:5672".to_string(),
            Arc::new(ParkingLotMutex::new(MemoryCache::new(
                1000,
                db.clone(),
                true,
            ))),
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
        let db = Arc::new(ParkingLotMutex::new(DB::new().unwrap()));
        let cache = Arc::new(ParkingLotMutex::new(MemoryCache::new(
            1000,
            db.clone(),
            true,
        )));
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
            let mut cache_guard = cache.lock();
            cache_guard.insert(test_message.clone());
        }

        match producer.connect().await {
            Ok(_) => assert!(false, "Should not connect to invalid host"),
            Err(_) => {
                let cache_guard = cache.lock();
                let messages = cache_guard.get_recent_messages(10);
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].exchange, "test_exchange");
                assert_eq!(messages[0].routing_key, "test.key");
            }
        }
    }

    #[tokio::test]
    async fn test_message_statistic_rely_on_local_rabbitmq() {
        let db = Arc::new(ParkingLotMutex::new(DB::new().unwrap()));
        let mut producer = AmqpProduceer::new(
            TEST_RABBITMQ_URL.to_string(),
            Arc::new(ParkingLotMutex::new(MemoryCache::new(
                1000,
                db.clone(),
                true,
            ))),
        );

        let result = producer
            .produce(
                "test_exchange",
                "topic",
                "test.key",
                "Test message".as_bytes(),
                false,
            )
            .await;

        let report = producer.generate_report(Duration::from_secs(1));

        assert!(!report.is_empty());
        let PeriodProduceReport {
            url,
            exchange,
            routing_key,
            produced,
            confirmed,
        } = &report[0];
        assert_eq!(url, TEST_RABBITMQ_URL);
        assert_eq!(exchange, "test_exchange");
        assert_eq!(routing_key, "test.key");
        assert_eq!(*produced, 1);
        assert_eq!(*confirmed, result.is_ok() as u64);
    }
}
