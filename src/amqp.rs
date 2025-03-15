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

pub struct AmqpPublisher {
    url: String,
    connection: Option<Connection>,
    channel: Option<Channel>,
    last_heartbeat: Option<Instant>,
    cache: Arc<StdMutex<MemoryCache>>,
    declared_exchanges: HashMap<String, ExchangeType>,
}

impl AmqpPublisher {
    pub fn new(url: String, cache: Arc<StdMutex<MemoryCache>>) -> Self {
        Self {
            url,
            connection: None,
            channel: None,
            last_heartbeat: None,
            cache,
            declared_exchanges: HashMap::new(),
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

            self.clear_connection_state();

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

    pub async fn publish(
        &mut self,
        exchange: &str,
        exchange_type: &str,
        routing_key: &str,
        message: &[u8],
        is_retry: bool,
    ) -> Result<()> {
        if !self.is_connected().await {
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
            "Message cached successfully - Exchange: {} ({}), RoutingKey: {}, Current cache size: {} messages",
            exchange,
            exchange_type_clone,
            routing_key,
            cache.size()
        );
        Ok(())
    }

    pub async fn is_connected(&self) -> bool {
        if let Some(last_heartbeat) = self.last_heartbeat {
            if last_heartbeat.elapsed() > Duration::from_secs(30) {
                warn!("Heartbeat check failed, reconnecting");
                return false;
            }
            true
        } else {
            false
        }
    }

    pub async fn retry_cached_messages(&mut self) -> Result<()> {
        if !self.is_connected().await {
            return Ok(());
        }

        let messages = {
            let cache = self.cache.lock().unwrap();
            cache.get_recent(1000)
        };

        for message in messages {
            match self
                .publish(
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

    pub async fn start_retry_task(publisher: Arc<Mutex<AmqpPublisher>>) {
        tokio::spawn(async move {
            loop {
                time::sleep(Duration::from_secs(5)).await;
                let mut publisher = publisher.lock().await;
                if let Err(e) = publisher.retry_cached_messages().await {
                    error!("Failed to retry cached messages: {}", e);
                }
            }
        });
    }

    pub async fn start_heartbeat_task(publisher: Arc<Mutex<AmqpPublisher>>) {
        tokio::spawn(async move {
            loop {
                time::sleep(Duration::from_secs(15)).await;
                let mut publisher = publisher.lock().await;
                if !publisher.is_connected().await {
                    if let Err(e) = publisher.connect().await {
                        error!("Failed to reconnect: {}", e);
                    }
                }
            }
        });
    }
}

pub struct AmqpConnectionManager {
    publishers: HashMap<String, Arc<Mutex<AmqpPublisher>>>,
    db: Arc<StdMutex<DB>>,
    cache: Arc<StdMutex<MemoryCache>>,
}

impl AmqpConnectionManager {
    pub fn new(db: Arc<StdMutex<DB>>, cache: Arc<StdMutex<MemoryCache>>) -> Self {
        Self {
            publishers: HashMap::new(),
            db,
            cache,
        }
    }

    pub fn get_db(&self) -> Arc<StdMutex<DB>> {
        self.db.clone()
    }

    pub async fn get_or_create_publisher(
        &mut self,
        url: String,
    ) -> Result<Arc<Mutex<AmqpPublisher>>> {
        let (host, username, password, port) = AmqpPublisher::parse_amqp_url(&url)?;
        if host.is_empty() || username.is_empty() || password.is_empty() || port == 0 {
            return Err(Box::new(AmqpError::ChannelUseError(format!(
                "Invalid AMQP URL {}",
                url
            ))) as Box<dyn Error + Send + Sync>);
        }

        if let Some(publisher) = self.publishers.get(&url) {
            return Ok(publisher.clone());
        }

        let publisher = Arc::new(Mutex::new(AmqpPublisher::new(
            url.clone(),
            self.cache.clone(),
        )));

        AmqpPublisher::start_heartbeat_task(publisher.clone()).await;
        AmqpPublisher::start_retry_task(publisher.clone()).await;

        self.publishers.insert(url, publisher.clone());
        Ok(publisher)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_RABBITMQ_URL: &str = "amqp://guest:guest@localhost:5672";

    #[tokio::test]
    async fn test_connect_to_local_rabbitmq() {
        let db = Arc::new(StdMutex::new(DB::new().unwrap()));
        let mut publisher = AmqpPublisher::new(
            TEST_RABBITMQ_URL.to_string(),
            Arc::new(StdMutex::new(MemoryCache::new(1000, db.clone()))),
        );

        match publisher.connect().await {
            Ok(_) => assert!(true),
            Err(e) => {
                println!("Failed to connect to local RabbitMQ: {}", e);
                assert!(false);
            }
        }
    }

    #[tokio::test]
    async fn test_publish_with_different_exchange_types() {
        let db = Arc::new(StdMutex::new(DB::new().unwrap()));
        let mut publisher = AmqpPublisher::new(
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
            match publisher
                .publish(exchange, ex_type, routing_key, message.as_bytes(), false)
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
        let mut publisher = AmqpPublisher::new(
            "amqp://guest:guest@non_existent_host:5672".to_string(),
            Arc::new(StdMutex::new(MemoryCache::new(1000, db.clone()))),
        );

        match publisher.connect().await {
            Ok(_) => {
                assert!(false, "Should not be able to connect to invalid host");
            }
            Err(_) => assert!(true),
        }
    }

    #[tokio::test]
    async fn test_publish_with_cache() {
        let db = Arc::new(StdMutex::new(DB::new().unwrap()));
        let cache = Arc::new(StdMutex::new(MemoryCache::new(1000, db.clone())));
        let mut publisher = AmqpPublisher::new(
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

        match publisher.connect().await {
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
}
