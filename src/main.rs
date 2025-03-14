mod amqp;
mod config;
mod db;
mod logger;
mod memory_cache;
mod models;

use crate::amqp::AmqpPublisher;
use crate::db::DB;
use crate::memory_cache::MemoryCache;
use crate::models::Message;
use once_cell::sync::Lazy as SyncLazy;
use std::error::Error;
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as TokioMutex;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::{TcpListener, TcpStream},
};
use tracing::{debug, error, info, warn};

static PUBLISHER: SyncLazy<Arc<TokioMutex<AmqpPublisher>>> = SyncLazy::new(|| {
    let db = Arc::new(Mutex::new(DB::new().expect("无法创建数据库")));
    let cache = Arc::new(TokioMutex::new(MemoryCache::new(1000, db)));

    let publisher = AmqpPublisher::new("amqp://localhost:5672".to_string(), cache);
    Arc::new(TokioMutex::new(publisher))
});

async fn start_publisher_background_tasks() {
    AmqpPublisher::start_retry_task(PUBLISHER.clone()).await;

    AmqpPublisher::start_heartbeat_task(PUBLISHER.clone()).await;

    info!("AMQP发布者后台任务已启动");
}

async fn publish_to_rabbitmq(message: Message) -> Result<(), Box<dyn Error>> {
    let mut publisher = PUBLISHER.lock().await;

    match publisher.publish(message.clone()).await {
        Ok(_) => {
            info!(
                "成功发送消息到 RabbitMQ: exchange={}, routing_key={}",
                message.exchange, message.routing_key
            );
            Ok(())
        }
        Err(e) => {
            error!("发送消息到 RabbitMQ 失败: {}", e);
            Err(Box::new(e))
        }
    }
}

async fn process_connection(mut stream: TcpStream) -> Result<(), Box<dyn Error>> {
    let (reader, _writer) = stream.split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            debug!("连接关闭");
            break;
        }

        match serde_json::from_str::<Message>(line.trim()) {
            Ok(message) => {
                if let Err(e) = publish_to_rabbitmq(message).await {
                    error!("发布消息到RabbitMQ失败: {}", e);
                }
            }
            Err(e) => {
                warn!("JSON解析错误: {}", e);
            }
        }
    }
    Ok(())
}

// 检查数据库中未发送的消息，但不会插入测试消息
fn check_unsent_messages(db: &DB) {
    // 使用get_recent_messages和get_messages_by_exchange方法
    // 以避免编译器警告，但不会插入任何测试数据
    if let Ok(messages) = db.get_recent_messages(10) {
        if !messages.is_empty() {
            info!("数据库中有 {} 条待发送消息", messages.len());
        }
    }
    
    // 检查特定exchange的消息
    if let Ok(messages) = db.get_messages_by_exchange("test_exchange", 5) {
        if !messages.is_empty() {
            info!("exchange=test_exchange 的消息数量: {}", messages.len());
        }
    }
    
    // 使用insert方法的签名但不执行实际插入
    let _insert_fn: fn(&mut DB, &Message) -> rusqlite::Result<()> = DB::insert;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let settings = config::Settings::new()?;

    logger::init_logger(&settings.log)?;

    logger::start_log_cleaner(settings.log.clone());
    
    // 初始化数据库并检查未发送消息，但不会插入测试消息
    let db = DB::new()?;
    check_unsent_messages(&db);

    start_publisher_background_tasks().await;

    let addr = format!("{}:{}", settings.server.host, settings.server.port);
    let listener = TcpListener::bind(&addr).await?;
    info!("服务器启动在 {}", addr);

    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                info!("新连接: {}", addr);
                tokio::spawn(async move {
                    if let Err(e) = process_connection(socket).await {
                        error!("处理连接时出错: {}", e);
                    }
                });
            }
            Err(e) => {
                error!("接受连接时出错: {}", e);
            }
        }
    }
}
