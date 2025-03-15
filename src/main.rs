mod amqp;
mod config;
mod db;
mod logger;
mod memory_cache;
mod models;

use crate::amqp::CONNECTION_MANAGER;
use crate::models::Message;
use std::error::Error;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::{TcpListener, TcpStream},
};
use tracing::{debug, error, info, warn};
use std::time::Duration;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

async fn start_publisher_background_tasks() {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            if let Err(e) = retry_cached_messages().await {
                error!("重试缓存消息失败: {}", e);
            }
        }
    });
}

async fn retry_cached_messages() -> Result<()> {
    let mut manager = CONNECTION_MANAGER.lock().await;
    let messages = {
        let db = manager.get_db();
        let db = db.lock().unwrap();
        db.get_recent_messages(100)?
    };

    for message in messages {
        match manager.get_or_create_publisher(message.url.clone()).await {
            Ok(publisher) => {
                let mut publisher = publisher.lock().await;
                if let Err(e) = publisher.publish(&message.exchange, &message.routing_key, message.message.as_bytes()).await {
                    error!("重试发送消息失败: {}", e);
                }
            }
            Err(e) => {
                error!("获取发布者失败: {}", e);
            }
        }
    }
    Ok(())
}

async fn publish_message(url: String, exchange: String, routing_key: String, message: String) -> Result<()> {
    let mut manager = CONNECTION_MANAGER.lock().await;
    let publisher = manager.get_or_create_publisher(url.clone()).await?;
    let mut publisher = publisher.lock().await;
    publisher.publish(&exchange, &routing_key, message.as_bytes()).await
}

async fn process_connection(mut stream: TcpStream) -> Result<()> {
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
                if let Err(e) = publish_message(
                    message.url.clone(),
                    message.exchange.clone(),
                    message.routing_key.clone(),
                    message.message.clone()
                ).await {
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

#[tokio::main]
async fn main() -> Result<()> {
    let settings = config::Settings::new()?;

    logger::init_logger(&settings.log)?;

    logger::start_log_cleaner(settings.log.clone());
    
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
