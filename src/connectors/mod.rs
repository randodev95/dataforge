//! # Data Connectors
//! 
//! This module provides a registry and traits for external data sources, 
//! supporting both Object Stores (S3/GCS/Azure) and ADBC-compliant databases.

pub mod object_store;
pub mod adbc;

use crate::error::{TitanError, Result};
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use crate::project::SourceConfig;
use std::sync::Arc;

/// A trait for registering external data sources into the DataFusion context.
#[async_trait]
pub trait Connector: Send + Sync {
    async fn register(&self, ctx: &SessionContext, name: &str, config: &SourceConfig) -> Result<()>;
}

/// Orchestrates the registration of multiple source types.
pub struct ConnectorRegistry {
    object_store: Arc<object_store::ObjectStoreConnector>,
    adbc: Arc<adbc::AdbcConnector>,
}

impl ConnectorRegistry {
    pub fn new() -> Self {
        Self {
            object_store: Arc::new(object_store::ObjectStoreConnector::new()),
            adbc: Arc::new(adbc::AdbcConnector::new()),
        }
    }

    pub async fn register_source(&self, ctx: &SessionContext, name: &str, config: &SourceConfig) -> Result<()> {
        match config.source_type.as_str() {
            "s3" | "gcs" | "azure" | "local" => {
                self.object_store.register(ctx, name, config).await
            }
            "adbc" | "postgres" | "snowflake" | "bigquery" => {
                self.adbc.register(ctx, name, config).await
            }
            _ => Err(TitanError::ValidationError(format!("Unsupported source type: {}", config.source_type))),
        }
    }
}

impl Default for ConnectorRegistry {
    fn default() -> Self {
        Self::new()
    }
}
