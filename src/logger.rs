use crate::config::LogSettings;
use flexi_logger::{Cleanup, Criterion, DeferredNow, Duplicate, FileSpec, LogSpecBuilder, Logger, Naming, Record};
use log::*;
use std::sync::atomic::{AtomicU8, Ordering};

static CURRENT_LOG_LEVEL: AtomicU8 = AtomicU8::new(2); // Default to Info (2)

fn custom_format(
    w: &mut dyn std::io::Write,
    now: &mut DeferredNow,
    record: &Record,
) -> Result<(), std::io::Error> {
    write!(
        w,
        "{} [{}] [{}:{}] {}",
        now.format("%Y-%m-%d %H:%M:%S%.3f"),
        record.level(),
        record.file().unwrap_or("<unknown>"),
        record.line().unwrap_or(0),
        &record.args()
    )
}

fn to_level_filter(level: &str) -> LevelFilter {
    match level.to_lowercase().as_str() {
        "error" => LevelFilter::Error,
        "warn" => LevelFilter::Warn,
        "info" => LevelFilter::Info,
        "debug" => LevelFilter::Debug,
        "trace" => LevelFilter::Trace,
        _ => LevelFilter::Info,
    }
}

fn level_filter_to_u8(level: LevelFilter) -> u8 {
    match level {
        LevelFilter::Off => 0,
        LevelFilter::Error => 1,
        LevelFilter::Warn => 2,
        LevelFilter::Info => 3,
        LevelFilter::Debug => 4,
        LevelFilter::Trace => 5,
    }
}

fn u8_to_level_filter(level: u8) -> LevelFilter {
    match level {
        0 => LevelFilter::Off,
        1 => LevelFilter::Error,
        2 => LevelFilter::Warn,
        3 => LevelFilter::Info,
        4 => LevelFilter::Debug,
        5 => LevelFilter::Trace,
        _ => LevelFilter::Info,
    }
}

pub fn init_logger(setting: &LogSettings) -> Result<(), String> {
    let level_filter = to_level_filter(&setting.level);

    // Store the initial log level
    CURRENT_LOG_LEVEL.store(level_filter_to_u8(level_filter), Ordering::Relaxed);

    let mut log_spec_builder = LogSpecBuilder::new();
    log_spec_builder.default(level_filter);
    let log_spec = log_spec_builder.build();
    let file_spec = FileSpec::default()
        .directory(&setting.dir)
        .basename("amqp-agent");    let _logger_handle = Logger::with(log_spec)
        .log_to_file(file_spec)
        .duplicate_to_stdout(Duplicate::All)
        .format(custom_format)
        .rotate(
            Criterion::Size(500_000_000),
            Naming::Numbers,
            Cleanup::KeepLogFiles(setting.max_kept_files as usize),
        )
        .start()
        .unwrap();

    Ok(())
}

pub fn set_logger_level(level: String) {
    let new_level_filter = to_level_filter(&level);
    let previous_level = u8_to_level_filter(CURRENT_LOG_LEVEL.load(Ordering::Relaxed));

    if new_level_filter != previous_level {
        // Instead of starting a new logger, update the existing one's level
        log::set_max_level(new_level_filter);
        CURRENT_LOG_LEVEL.store(level_filter_to_u8(new_level_filter), Ordering::Relaxed);
        log::info!(
            "Log level changed from {:?} to {:?}",
            previous_level,
            new_level_filter
        );
    }
}
