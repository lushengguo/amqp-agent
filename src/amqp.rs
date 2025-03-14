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

const PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);
const RETRY_INTERVAL: Duration = Duration::from_secs(30);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const MAX_CONNECT_RETRIES: u32 = 3;

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
    pub fn success_rate(&self) -> f64 {
        if self.total_messages == 0 {
            0.0
        } else {
            self.successful_messages as f64 / self.total_messages as f64
        }
    }

    pub fn average_latency(&self) -> Duration {
        if self.successful_messages == 0 {
            Duration::from_secs(0)
        } else {
            Duration::from_millis(self.total_latency / self.successful_messages)
        }
    }

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

    pub fn get_stats(&self) -> &PublishStats {
        &self.stats
    }

    async fn do_connect(&mut self) -> Result<(), AmqpError> {
        let args = OpenConnectionArguments::new(&self.url, "amqp-agent", "/");
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
            let mut cache: tokio::sync::MutexGuard<'_, MemoryCache> = self.cache.lock().await;
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

    pub async fn publish_batch(&mut self, messages: Vec<Message>) -> Result<(), AmqpError> {
        for message in messages {
            if let Err(e) = self.publish(message).await {
                warn!("批量发送消息时失败: {}", e);

                continue;
            }
        }
        Ok(())
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
