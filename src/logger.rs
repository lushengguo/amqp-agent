use crate::config::LogConfig;
use std::{
    error::Error,
    fs,
    time::{SystemTime, UNIX_EPOCH},
};
use tracing::{error, info, Level};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{fmt::Layer, util::SubscriberInitExt, EnvFilter};

pub fn init_logger(log_config: &LogConfig) -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(&log_config.directory)?;

    if let Err(e) = cleanup_old_logs(
        &log_config.directory,
        &log_config.prefix,
        log_config.max_days,
    ) {
        eprintln!("清理旧日志文件时出错: {}", e);
    }

    let log_level = match log_config.level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    let file_appender =
        RollingFileAppender::new(Rotation::DAILY, &log_config.directory, &log_config.prefix);

    let console_layer = Layer::new()
        .with_target(false)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true);

    let file_layer = Layer::new()
        .with_ansi(false)
        .with_target(false)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .with_writer(file_appender);

    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env().add_directive(log_level.into()))
        .with(console_layer)
        .with(file_layer)
        .init();

    Ok(())
}

pub fn start_log_cleaner(log_config: LogConfig) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(24 * 60 * 60));
        loop {
            interval.tick().await;
            if let Err(e) = cleanup_old_logs(
                &log_config.directory,
                &log_config.prefix,
                log_config.max_days,
            ) {
                error!("定期清理日志文件时出错: {}", e);
            }
        }
    });
}

fn cleanup_old_logs(directory: &str, prefix: &str, max_days: u64) -> Result<(), Box<dyn Error>> {
    let max_age = max_days * 24 * 60 * 60;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();

        if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
            if file_name.starts_with(prefix) && file_name.ends_with(".log") {
                if let Ok(metadata) = fs::metadata(&path) {
                    if let Ok(modified) = metadata.modified() {
                        let age = now - modified.duration_since(UNIX_EPOCH)?.as_secs();
                        if age > max_age {
                            fs::remove_file(&path)?;
                            info!("已删除过期日志文件: {}", path.display());
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
