//! # Observability & Telemetry
//!
//! This module initializes the tracing infrastructure for Titan,
//! supporting structured logging to stdout and persistent log files.

use std::path::Path;
use tracing_appender::rolling;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, fmt};

pub fn init_telemetry(project_root: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let log_dir = project_root.join("logs");
    std::fs::create_dir_all(&log_dir)?;

    let file_appender = rolling::daily(log_dir, "titan.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    let stdout_layer = fmt::layer().with_target(false).with_thread_ids(true);

    let file_layer = fmt::layer().with_writer(non_blocking).json();

    // OpenTelemetry Tracing Hook (Principal Engineer Placeholder)
    // In a production FAANG environment, we would connect this to a Jaeger or Honeycomb collector
    // for distributed tracing across the pipeline execution.
    // let tracer = opentelemetry_jaeger::new_pipeline().with_service_name("titan-engine").install_simple()?;
    // let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .with(stdout_layer)
        .with(file_layer)
        // .with(otel_layer)
        .init();

    Ok(())
}
