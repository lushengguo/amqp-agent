use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use amqprs::{
    channel::{BasicPublishArguments, Channel, ConfirmSelectArguments},
    connection::{Connection, OpenConnectionArguments},
    BasicProperties,
    error::Error as AmqpError,
};
use tokio::sync::Mutex;
use tokio::time::{self, Instant};
use tracing::{error, info, warn};
use url::Url;
use once_cell::sync::Lazy;

use crate::memory_cache::MemoryCache;
use crate::db::DB;
use crate::models::Message;
use super::models::Message as AmqpMessage;
use std::{future::Future, pin::Pin};
use std::time::{SystemTime, UNIX_EPOCH};
use std::error::Error;

const PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);
const RETRY_INTERVAL: Duration = Duration::from_secs(30);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const MAX_CONNECT_RETRIES: u32 = 3;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

pub static CONNECTION_MANAGER: Lazy<Mutex<AmqpConnectionManager>> = Lazy::new(|| {
    let db = Arc::new(StdMutex::new(DB::new().expect("Failed to create database")));
    Mutex::new(AmqpConnectionManager::new(db))
});

pub struct AmqpPublisher {
    url: String,
    connection: Option<Connection>,
    channel: Option<Channel>,
    last_heartbeat: Option<Instant>,
    cache: Arc<StdMutex<MemoryCache>>,
    db: Arc<StdMutex<DB>>,
}

impl AmqpPublisher {
    pub fn new(url: String, db: Arc<StdMutex<DB>>) -> Self {
        let cache = Arc::new(StdMutex::new(MemoryCache::new(1000, db.clone())));
        Self {
            url,
            connection: None,
            channel: None,
            last_heartbeat: None,
            cache,
            db,
        }
    }

    fn parse_amqp_url(url_str: &str) -> Result<(String, String, String)> {
        let url = Url::parse(url_str).map_err(|e| {
            Box::new(AmqpError::ChannelUseError(format!("Invalid AMQP URL: {}", e))) as Box<dyn Error + Send + Sync>
        })?;

        if url.scheme() != "amqp" {
            return Err(Box::new(AmqpError::ChannelUseError(
                "URL scheme must be 'amqp'".to_string(),
            )) as Box<dyn Error + Send + Sync>);
        }

        let host = format!(
            "{}:{}",
            url.host_str().unwrap_or("localhost"),
            url.port().unwrap_or(5672)
        );
        let username = url.username().to_string();
        let password = url.password().unwrap_or("").to_string();

        Ok((host, username, password))
    }

    async fn do_connect(&mut self) -> Result<()> {
        let (host, username, password) = Self::parse_amqp_url(&self.url)?;
        let args = OpenConnectionArguments::new(&host, &username, &password);

        self.connection = Some(Connection::open(&args).await?);
        self.channel = Some(self.connection.as_ref().unwrap().open_channel(None).await?);
        self.channel
            .as_ref()
            .unwrap()
            .confirm_select(ConfirmSelectArguments::default())
            .await?;

        self.last_heartbeat = Some(Instant::now());
        Ok(())
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
                        let error_msg = format!("连接 RabbitMQ 失败: {}", e);
                        warn!("{}，将在 3 秒后重试", error_msg);
                        time::sleep(Duration::from_secs(3)).await;
                        retries += 1;
                    }
                }
            }

            Err(Box::new(AmqpError::ChannelUseError("无法建立连接".to_string())) as Box<dyn Error + Send + Sync>)
        })
    }

    fn ensure_connection(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            if self.connection.is_none() || !self.is_connected().await {
                self.connect().await?;
            }
            Ok(())
        })
    }

    pub async fn publish(&mut self, exchange: &str, routing_key: &str, message: &[u8]) -> Result<()> {
        if !self.is_connected().await {
            self.connect().await?;
        }

        let args = BasicPublishArguments::new(exchange, routing_key);
        match self
            .channel
            .as_ref()
            .unwrap()
            .basic_publish(BasicProperties::default(), message.to_vec(), args)
            .await
        {
            Ok(_) => {
                info!("消息发送成功");
                Ok(())
            }
            Err(e) => {
                let error_msg = format!("发送消息失败: {}", e);
                error!("{}", error_msg);
                self.cache_message(exchange, routing_key, message).await?;
                Err(Box::new(AmqpError::ChannelUseError(error_msg)) as Box<dyn Error + Send + Sync>)
            }
        }
    }

    async fn cache_message(&self, exchange: &str, routing_key: &str, message: &[u8]) -> Result<()> {
        let message = Message {
            url: self.url.clone(),
            exchange: exchange.to_string(),
            routing_key: routing_key.to_string(),
            message: String::from_utf8_lossy(message).to_string(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as u32,
        };

        let mut cache = self.cache.lock().unwrap();
        cache.insert(message);
        Ok(())
    }

    pub async fn is_connected(&self) -> bool {
        if let Some(last_heartbeat) = self.last_heartbeat {
            if last_heartbeat.elapsed() > Duration::from_secs(30) {
                warn!("心跳检测失败，重新建立连接");
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
            cache.get_recent(cache.size())
        };

        for message in messages {
            match self
                .publish(&message.exchange, &message.routing_key, message.message.as_bytes())
                .await
            {
                Ok(_) => {
                    let mut cache = self.cache.lock().unwrap();
                    cache.remove(message.timestamp);
                }
                Err(e) => {
                    error!("重试发送消息失败: {}", e);
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
                    error!("重试缓存消息失败: {}", e);
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
                        error!("重新连接失败: {}", e);
                    }
                }
            }
        });
    }
}

pub struct AmqpConnectionManager {
    publishers: HashMap<String, Arc<Mutex<AmqpPublisher>>>,
    db: Arc<StdMutex<DB>>,
}

impl AmqpConnectionManager {
    pub fn new(db: Arc<StdMutex<DB>>) -> Self {
        Self {
            publishers: HashMap::new(),
            db,
        }
    }

    pub fn get_db(&self) -> Arc<StdMutex<DB>> {
        self.db.clone()
    }

    pub async fn get_or_create_publisher(&mut self, url: String) -> Result<Arc<Mutex<AmqpPublisher>>> {
        if let Some(publisher) = self.publishers.get(&url) {
            Ok(publisher.clone())
        } else {
            let publisher = AmqpPublisher::new(url.clone(), self.db.clone());
            let publisher = Arc::new(Mutex::new(publisher));
            self.publishers.insert(url, publisher.clone());
            Ok(publisher)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    const TEST_RABBITMQ_URL: &str = "amqp://guest:guest@localhost:5672";

    #[tokio::test]
    async fn test_connect_to_local_rabbitmq() {
        let db = Arc::new(StdMutex::new(DB::new().unwrap()));
        let mut publisher = AmqpPublisher::new(
            TEST_RABBITMQ_URL.to_string(),
            db.clone(),
        );

        match publisher.connect().await {
            Ok(_) => assert!(true),
            Err(e) => {
                println!("连接到本地RabbitMQ失败: {}", e);
                assert!(false);
            }
        }
    }

    #[tokio::test]
    async fn test_connect_to_invalid_host() {
        let db = Arc::new(StdMutex::new(DB::new().unwrap()));
        let mut publisher = AmqpPublisher::new(
            "amqp://guest:guest@non_existent_host:5672".to_string(),
            db.clone(),
        );

        match publisher.connect().await {
            Ok(_) => assert!(false, "应该连接失败"),
            Err(_) => assert!(true),
        }
    }

    #[tokio::test]
    async fn test_publish_with_cache() {
        let db = Arc::new(StdMutex::new(DB::new().unwrap()));
        let mut publisher = AmqpPublisher::new(
            TEST_RABBITMQ_URL.to_string(),
            db.clone(),
        );

        let exchange = "test_exchange";
        let routing_key = "test_key";
        let message = "test message";

        match publisher.publish(exchange, routing_key, message.as_bytes()).await {
            Ok(_) => assert!(true),
            Err(e) => {
                println!("发布消息失败: {}", e);
                assert!(false);
            }
        }
    }
}
