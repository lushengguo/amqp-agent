use crate::config::LogSettings;
use std::fs;
use std::path::Path;
use tracing::Level;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{
    fmt::writer::MakeWriterExt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter,
};

pub fn init_logger(
    config: &LogSettings,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let level = match config.level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    // 创建日志目录（如果不存在）
    if !Path::new(&config.dir).exists() {
        fs::create_dir_all(&config.dir)?;
    }

    // 配置每日轮转的日志文件
    let file_appender = RollingFileAppender::builder()
        .rotation(Rotation::DAILY)
        .filename_prefix("app")
        .filename_suffix("log")
        .build(&config.dir)?;

    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking.with_max_level(level));

    let stdout_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stdout.with_max_level(level));

    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env().add_directive(level.into()))
        .with(file_layer)
        .with(stdout_layer)
        .init();

    Ok(())
}

pub fn start_log_cleaner(config: LogSettings) {
    tokio::spawn(async move {
        loop {
            if let Err(e) = clean_old_logs(&config).await {
                tracing::error!("Error cleaning old log files: {}", e);
            }
            // 每天检查一次
            tokio::time::sleep(tokio::time::Duration::from_secs(86400)).await;
        }
    });
}

async fn clean_old_logs(
    config: &LogSettings,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let log_dir = Path::new(&config.dir);
    if !log_dir.exists() {
        return Ok(());
    }

    let entries = fs::read_dir(log_dir)?;
    let max_files = config.max_files;
    let mut files: Vec<_> = entries
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry.path().extension().map_or(false, |ext| {
                ext.to_string_lossy() == "log"
            })
        })
        .collect();

    // 按修改时间排序，最新的在后面
    files.sort_by_key(|entry| entry.metadata().unwrap().modified().unwrap());

    // 如果文件数超过限制，删除最旧的文件
    if files.len() > max_files as usize {
        for file in files.iter().take(files.len() - max_files as usize) {
            if let Err(e) = fs::remove_file(file.path()) {
                tracing::error!("Error deleting old log file: {}", e);
            } else {
                tracing::info!("Deleted old log file: {}", file.path().display());
            }
        }
    }

    Ok(())
}
