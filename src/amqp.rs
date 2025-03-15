use super::memory_cache::MemoryCache;
use super::models::Message;
use amqprs::{
    channel::{BasicPublishArguments, Channel, ConfirmSelectArguments},
    connection::{Connection, OpenConnectionArguments},
    error::Error as AmqpError,
    BasicProperties,
};
use std::{future::Future, pin::Pin, sync::Arc};
use tokio::time::{self, timeout, Duration, Instant};
use tracing::{error, info, warn};
use url::Url;

const PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);
const RETRY_INTERVAL: Duration = Duration::from_secs(30);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const MAX_CONNECT_RETRIES: u32 = 3;

fn parse_amqp_url(url_str: &str) -> Result<(String, String, String), AmqpError> {
    let url = Url::parse(url_str).map_err(|e| {
        AmqpError::ChannelUseError(format!("Invalid AMQP URL: {}", e))
    })?;

    if url.scheme() != "amqp" {
        return Err(AmqpError::ChannelUseError(
            "URL scheme must be 'amqp'".to_string(),
        ));
    }

    let host = format!("{}:{}", url.host_str().unwrap_or("localhost"), 
                               url.port().unwrap_or(5672));
    let username = url.username().to_string();
    let password = url.password().unwrap_or("guest").to_string();

    Ok((host, username, password))
}

#[derive(Debug, Default)]
pub struct PublishStats {
    pub total_messages: u64,
    pub successful_messages: u64,
    pub failed_messages: u64,
    pub retried_messages: u64,
    pub total_latency: u64,
    pub last_heartbeat: Option<Instant>,
}

impl PublishStats {
    pub fn update_heartbeat(&mut self) {
        self.last_heartbeat = Some(Instant::now());
    }

    pub fn is_connection_alive(&self) -> bool {
        self.last_heartbeat
            .map(|t| t.elapsed() < HEARTBEAT_INTERVAL * 2)
            .unwrap_or(false)
    }
}

pub struct AmqpPublisher {
    connection: Option<Connection>,
    channel: Option<Channel>,
    cache: Arc<tokio::sync::Mutex<MemoryCache>>,
    url: String,
    stats: PublishStats,
}

impl AmqpPublisher {
    pub fn new(url: String, cache: Arc<tokio::sync::Mutex<MemoryCache>>) -> Self {
        Self {
            connection: None,
            channel: None,
            cache,
            url,
            stats: PublishStats::default(),
        }
    }

    async fn do_connect(&mut self) -> Result<(), AmqpError> {
        let (host, username, password) = parse_amqp_url(&self.url)?;
        let args = OpenConnectionArguments::new(&host, &username, &password);
        let connection = Connection::open(&args).await?;
        let channel = connection.open_channel(None).await?;

        channel
            .confirm_select(ConfirmSelectArguments::default())
            .await?;

        self.connection = Some(connection);
        self.channel = Some(channel);
        self.stats.update_heartbeat();

        Ok(())
    }

    fn connect(&mut self) -> Pin<Box<dyn Future<Output = Result<(), AmqpError>> + Send + '_>> {
        Box::pin(async move {
            if self.connection.is_some() {
                return Ok(());
            }

            let mut retries = 0;
            while retries < MAX_CONNECT_RETRIES {
                match self.do_connect().await {
                    Ok(_) => {
                        info!("成功连接到 RabbitMQ");

                        self.retry_cached_messages().await;
                        return Ok(());
                    }
                    Err(e) => {
                        retries += 1;
                        if retries == MAX_CONNECT_RETRIES {
                            error!("连接 RabbitMQ 失败，已达到最大重试次数: {}", e);
                            return Err(e);
                        }
                        warn!("连接 RabbitMQ 失败，将在 3 秒后重试: {}", e);
                        time::sleep(Duration::from_secs(3)).await;
                    }
                }
            }

            Err(AmqpError::ChannelUseError("无法建立连接".to_string()))
        })
    }

    fn ensure_connection(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), AmqpError>> + Send + '_>> {
        Box::pin(async move {
            if self.connection.is_none() || !self.stats.is_connection_alive() {
                if self.connection.is_some() {
                    warn!("心跳检测失败，重新建立连接");
                }
                self.connection = None;
                self.channel = None;
                self.connect().await?;
            }
            Ok(())
        })
    }

    pub async fn publish(&mut self, message: Message) -> Result<(), AmqpError> {
        self.stats.total_messages += 1;
        let start_time = Instant::now();

        if let Err(e) = self.ensure_connection().await {
            let mut cache = self.cache.lock().await;
            cache.insert(message);
            self.stats.failed_messages += 1;
            return Err(e);
        }

        let channel = self.channel.as_ref().unwrap();
        let args = BasicPublishArguments::new(&message.exchange, &message.routing_key);
        let props = BasicProperties::default();
        let body = message.message.as_bytes().to_vec();

        match timeout(PUBLISH_TIMEOUT, channel.basic_publish(props, body, args)).await {
            Ok(result) => match result {
                Ok(_) => {
                    let latency = start_time.elapsed();
                    self.stats.successful_messages += 1;
                    self.stats.total_latency += latency.as_millis() as u64;
                    self.stats.update_heartbeat();
                    info!(
                        "消息发送成功: exchange={}, routing_key={}, latency={:?}",
                        message.exchange, message.routing_key, latency
                    );
                    Ok(())
                }
                Err(e) => {
                    warn!("消息发送失败: {}", e);

                    let mut cache = self.cache.lock().await;
                    cache.insert(message);

                    self.connection = None;
                    self.channel = None;
                    self.stats.failed_messages += 1;
                    Err(e)
                }
            },
            Err(_) => {
                warn!("消息发送超时");

                let mut cache = self.cache.lock().await;
                cache.insert(message);

                self.connection = None;
                self.channel = None;
                self.stats.failed_messages += 1;
                Err(AmqpError::ChannelUseError("发送超时".to_string()))
            }
        }
    }

    async fn retry_cached_messages(&mut self) {
        let mut timestamps_to_remove = Vec::new();

        let messages = {
            let cache = self.cache.lock().await;
            cache.get_recent(cache.size())
        };

        for message in messages {
            self.stats.retried_messages += 1;
            match self.publish(message.clone()).await {
                Ok(_) => {
                    timestamps_to_remove.push(message.timestamp);
                }
                Err(e) => {
                    error!("重试发送消息失败: {}", e);
                    break;
                }
            }
        }

        if !timestamps_to_remove.is_empty() {
            let mut cache = self.cache.lock().await;
            cache.remove_batch(&timestamps_to_remove);
        }
    }

    pub async fn start_retry_task(publisher: Arc<tokio::sync::Mutex<AmqpPublisher>>) {
        tokio::spawn(async move {
            loop {
                time::sleep(RETRY_INTERVAL).await;
                let mut publisher = publisher.lock().await;
                publisher.retry_cached_messages().await;
            }
        });
    }

    pub async fn start_heartbeat_task(publisher: Arc<tokio::sync::Mutex<AmqpPublisher>>) {
        tokio::spawn(async move {
            loop {
                time::sleep(HEARTBEAT_INTERVAL).await;
                let mut publisher = publisher.lock().await;
                if let Err(e) = publisher.ensure_connection().await {
                    error!("心跳检测失败: {}", e);
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Message;
    use std::time::{SystemTime, UNIX_EPOCH};
    use crate::db::DB;
    use std::sync::Mutex;

    fn setup_test_db() -> Arc<Mutex<DB>> {
        let db = DB::new().unwrap();
        Arc::new(Mutex::new(db))
    }

    async fn wait_for_rabbitmq(url: &str, max_retries: u32) -> bool {
        for _ in 0..max_retries {
            let (host, username, password) = match parse_amqp_url(url) {
                Ok(v) => v,
                Err(_) => continue,
            };
            
            let args = OpenConnectionArguments::new(&host, &username, &password);
            match Connection::open(&args).await {
                Ok(_) => return true,
                Err(_) => {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            }
        }
        false
    }

    #[tokio::test]
    async fn test_connect_to_local_rabbitmq() {
        let rabbitmq_url = "amqp://guest:guest@127.0.0.1:5672";
        
        // 等待 RabbitMQ 可用
        if !wait_for_rabbitmq(rabbitmq_url, 5).await {
            println!("警告: RabbitMQ 服务器不可用，跳过测试");
            return;
        }

        // 创建测试数据库和缓存
        let db = setup_test_db();
        let cache = Arc::new(tokio::sync::Mutex::new(MemoryCache::new(100, db.clone())));
        
        // 创建发布者实例
        let mut publisher = AmqpPublisher::new(
            rabbitmq_url.to_string(),
            cache.clone(),
        );

        // 测试连接
        let result = publisher.ensure_connection().await;
        assert!(result.is_ok(), "连接到本地RabbitMQ失败: {:?}", result);

        // 创建测试消息
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;
            
        let test_message = Message {
            url: rabbitmq_url.to_string(),
            exchange: "test_exchange".to_string(),
            routing_key: "test_key".to_string(),
            message: "测试消息".to_string(),
            timestamp,
        };

        // 测试发布消息
        let publish_result = publisher.publish(test_message).await;
        assert!(publish_result.is_ok(), "发布消息失败: {:?}", publish_result);

        // 检查统计信息
        assert_eq!(publisher.stats.total_messages, 1);
        assert_eq!(publisher.stats.successful_messages, 1);
        assert_eq!(publisher.stats.failed_messages, 0);
        assert!(publisher.stats.last_heartbeat.is_some());
    }

    #[tokio::test]
    async fn test_connection_retry() {
        let db = setup_test_db();
        let cache = Arc::new(tokio::sync::Mutex::new(MemoryCache::new(100, db.clone())));
        let mut publisher = AmqpPublisher::new(
            "amqp://guest:guest@non_existent_host:5672".to_string(),
            cache.clone(),
        );

        // 测试连接重试
        let result = publisher.ensure_connection().await;
        assert!(result.is_err(), "应该无法连接到不存在的主机");
        assert_eq!(publisher.stats.failed_messages, 0);
    }

    #[tokio::test]
    async fn test_publish_with_cache() {
        let rabbitmq_url = "amqp://guest:guest@127.0.0.1:5672";
        
        // 等待 RabbitMQ 可用
        if !wait_for_rabbitmq(rabbitmq_url, 5).await {
            println!("警告: RabbitMQ 服务器不可用，跳过测试");
            return;
        }

        let db = setup_test_db();
        let cache = Arc::new(tokio::sync::Mutex::new(MemoryCache::new(100, db.clone())));
        let mut publisher = AmqpPublisher::new(
            rabbitmq_url.to_string(),
            cache.clone(),
        );

        // 创建测试消息
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;
            
        let test_message = Message {
            url: rabbitmq_url.to_string(),
            exchange: "test_exchange".to_string(),
            routing_key: "test_key".to_string(),
            message: "测试缓存消息".to_string(),
            timestamp,
        };

        // 确保连接
        let _ = publisher.ensure_connection().await;

        // 发布消息
        let result = publisher.publish(test_message.clone()).await;
        assert!(result.is_ok(), "发布消息失败: {:?}", result);

        // 检查缓存
        let cache_lock = cache.lock().await;
        assert_eq!(cache_lock.size(), 0, "成功发送的消息不应该在缓存中");
    }
}
