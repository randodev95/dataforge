use crate::error::{TitanError, Result};
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use object_store::aws::AmazonS3Builder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::local::LocalFileSystem;
use crate::project::SourceConfig;
use std::sync::Arc;
use url::Url;

pub struct ObjectStoreConnector;

impl ObjectStoreConnector {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ObjectStoreConnector {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl super::Connector for ObjectStoreConnector {
    async fn register(&self, ctx: &SessionContext, name: &str, config: &SourceConfig) -> Result<()> {
        let url = Url::parse(&format!("{}://{}", config.source_type, name))
            .map_err(|e| TitanError::ValidationError(format!("Invalid URL for source {}: {}", name, e)))?;
        
        match config.source_type.as_str() {
            "s3" => {
                let bucket = config.bucket.as_ref().ok_or_else(|| TitanError::ValidationError("Bucket required for S3".to_string()))?;
                let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
                if let Some(region) = &config.region {
                    builder = builder.with_region(region);
                }
                if let Some(endpoint) = &config.endpoint {
                    builder = builder.with_endpoint(endpoint);
                }
                let store = Arc::new(builder.build().map_err(|e| TitanError::DatabaseError(e.to_string()))?);
                ctx.runtime_env().register_object_store(&url, store);
            }
            "gcs" => {
                let bucket = config.bucket.as_ref().ok_or_else(|| TitanError::ValidationError("Bucket required for GCS".to_string()))?;
                let builder = GoogleCloudStorageBuilder::from_env().with_bucket_name(bucket);
                let store = Arc::new(builder.build().map_err(|e| TitanError::DatabaseError(e.to_string()))?);
                ctx.runtime_env().register_object_store(&url, store);
            }
            "azure" => {
                let bucket = config.bucket.as_ref().ok_or_else(|| TitanError::ValidationError("Container required for Azure".to_string()))?;
                let builder = MicrosoftAzureBuilder::from_env().with_container_name(bucket);
                let store = Arc::new(builder.build().map_err(|e| TitanError::DatabaseError(e.to_string()))?);
                ctx.runtime_env().register_object_store(&url, store);
            }
            "local" => {
                let store = Arc::new(LocalFileSystem::new());
                ctx.runtime_env().register_object_store(&url, store);
            }
            _ => return Err(TitanError::ValidationError(format!("Unsupported object store type: {}", config.source_type))),
        }
        Ok(())
    }
}
