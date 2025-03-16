mod amqp;
mod config;
mod db;
mod logger;
mod memory_cache;
mod models;

use crate::amqp::CONNECTION_MANAGER;
use crate::models::Message;
use parking_lot::deadlock;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::SystemTime;
use tokio::sync::Mutex;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::{TcpListener, TcpStream},
    time,
};
use tracing::{debug, error, info, warn};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Debug, Clone)]
struct MessageStats {
    count: u64,
    timestamp: SystemTime,
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
struct StatsKey {
    ip_port: String,
    exchange: String,
    routing_key: String,
}

impl StatsKey {
    fn new(ip_port: String, exchange: String, routing_key: String) -> Self {
        Self {
            ip_port,
            exchange,
            routing_key,
        }
    }
}

lazy_static::lazy_static! {
    static ref MESSAGE_STATS: Arc<Mutex<HashMap<StatsKey, MessageStats>>> = Arc::new(Mutex::new(HashMap::new()));
}

async fn update_stats(url: &str, exchange: &str, routing_key: &str) {
    let mut stats = MESSAGE_STATS.lock().await;
    let key = StatsKey::new(
        url.to_string(),
        exchange.to_string(),
        routing_key.to_string(),
    );

    let entry = stats.entry(key).or_insert(MessageStats {
        count: 0,
        timestamp: SystemTime::now(),
    });

    entry.count += 1;
    entry.timestamp = SystemTime::now();
}

async fn start_producer_background_tasks() {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            if let Err(e) = retry_message_count().await {
                error!("Failed to retry cached messages: {}", e);
            }
        }
    });
}

async fn retry_message_count() -> Result<()> {
    let mut manager = CONNECTION_MANAGER.lock().await;
    let messages = {
        let db = manager.get_db();
        let db = db.lock();
        let db_messages = db.get_recent_messages(10).unwrap();
        db_messages
    };

    for message in messages {
        match manager.get_or_create_producer(message.url.clone()).await {
            Ok(producer) => {
                let mut producer = producer.lock().await;
                if let Err(e) = producer
                    .produce(
                        &message.exchange,
                        &message.exchange_type,
                        &message.routing_key,
                        message.message.as_bytes(),
                        true,
                    )
                    .await
                {
                    error!("Failed to retry message: {}", e);
                }
            }
            Err(e) => {
                error!("Failed to get producer: {}", e);
            }
        }
    }
    Ok(())
}

async fn produce_message(
    url: String,
    exchange: String,
    exchange_type: String,
    routing_key: String,
    message: String,
) -> Result<()> {
    let mut manager = CONNECTION_MANAGER.lock().await;
    let producer = manager.get_or_create_producer(url.clone()).await?;
    let mut producer = producer.lock().await;
    update_stats(&url, &exchange, &routing_key).await;
    producer
        .produce(
            &exchange,
            &exchange_type,
            &routing_key,
            message.as_bytes(),
            false,
        )
        .await
}

async fn process_connection(mut stream: TcpStream) -> Result<()> {
    let (reader, _writer) = stream.split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            debug!("Connection closed");
            break;
        }

        match serde_json::from_str::<Message>(line.trim()) {
            Ok(message) => {
                if let Err(e) = produce_message(
                    message.url.clone(),
                    message.exchange.clone(),
                    message.exchange_type.clone(),
                    message.routing_key.clone(),
                    message.message.clone(),
                )
                .await
                {
                    error!("Failed to produce message to RabbitMQ: {}", e);
                }
            }
            Err(e) => {
                warn!("JSON parsing error: {}", e);
            }
        }
    }
    Ok(())
}

async fn start_report_task() {
    tokio::spawn(async move {
        loop {
            time::sleep(Duration::from_secs(60)).await;
            let manager = CONNECTION_MANAGER.lock().await;
            let (produce_reports, cache_report) = manager.generate_reports().await;

            for report in produce_reports {
                info!(
                    "PRODUCE REPORT - url:{}, exchange:{}, routingkey:{}, produceed:{}, succeed:{}",
                    report.url,
                    report.exchange,
                    report.routing_key,
                    report.total_sent,
                    report.total_success
                );
            }

            info!(
                "CACHE REPORT - memory cached {} messages, memory usage: {},  disk cached {} messages, disk usage: {}",
                cache_report.in_memory_message_count, cache_report.memory_usage,  cache_report.in_disk_message_count, cache_report.disk_usage
            );
        }
    });
}

fn start_dead_lock_detection() {
    tokio::spawn(async move {
        loop {
            thread::sleep(Duration::from_secs(1));
            let deadlocks = deadlock::check_deadlock();
            if deadlocks.is_empty() {
                continue;
            }
            debug!("detect {} deadlock!", deadlocks.len());
            for (i, threads) in deadlocks.iter().enumerate() {
                println!("deadlock {} related threads:", i);
                for thread in threads {
                    error!("{:?}", thread.backtrace());
                }
            }
        }
    });
}

#[tokio::main]
async fn main() -> Result<()> {
    let settings = config::Settings::new()?;
    logger::init_logger(&settings.log)?;
    logger::start_log_cleaner(settings.log.clone());

    start_dead_lock_detection();
    start_producer_background_tasks().await;
    start_report_task().await;

    let addr = format!("{}:{}", settings.server.host, settings.server.port);
    let listener = TcpListener::bind(&addr).await?;
    info!("Server started on {}", addr);

    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                info!("New connection: {}", addr);
                tokio::spawn(async move {
                    if let Err(e) = process_connection(socket).await {
                        error!("Error processing connection: {}", e);
                    }
                });
            }
            Err(e) => {
                error!("Error accepting connection: {}", e);
            }
        }
    }
}
