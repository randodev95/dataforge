//! # Observability & Telemetry
//! 
//! This module initializes the tracing infrastructure for Titan, 
//! supporting structured logging to stdout and persistent log files.

use tracing_subscriber::{fmt, EnvFilter};
use tracing_appender::rolling;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use std::path::Path;

pub fn init_telemetry(project_root: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let log_dir = project_root.join("logs");
    std::fs::create_dir_all(&log_dir)?;

    let file_appender = rolling::daily(log_dir, "titan.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    let stdout_layer = fmt::layer()
        .with_target(false)
        .with_thread_ids(true);

    let file_layer = fmt::layer()
        .with_writer(non_blocking)
        .json();

    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env()
            .add_directive(tracing::Level::INFO.into()))
        .with(stdout_layer)
        .with(file_layer)
        .init();

    Ok(())
}
