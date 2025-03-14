mod config;
mod logger;
mod amqp;

use tokio::{
    net::{TcpListener, TcpStream},
    io::{BufReader, AsyncBufReadExt},
};
use std::error::Error;
use tracing::{info, warn, error, debug};
use crate::amqp::Message;

async fn dummy_func(message: Message) {
    info!("收到消息: url={}, exchange={}, routing_key={}, message={}, timestamp={}", 
        message.url, message.exchange, message.routing_key, message.message, message.timestamp);
    // 这里是您后续要实现的函数
}

async fn process_connection(mut stream: TcpStream) -> Result<(), Box<dyn Error>> {
    let (reader, _writer) = stream.split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            // 连接已关闭
            debug!("连接关闭");
            break;
        }

        // 尝试解析JSON为Message结构体
        match serde_json::from_str::<Message>(line.trim()) {
            Ok(message) => {
                dummy_func(message).await;
            }
            Err(e) => {
                warn!("JSON解析错误: {}", e);
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // 加载配置
    let settings = config::Settings::new()?;
    
    // 初始化日志系统
    logger::init_logger(&settings.log)?;
    
    // 启动日志清理任务
    logger::start_log_cleaner(settings.log.clone());

    // 启动服务器
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
