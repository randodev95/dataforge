use crate::error::{Result, TitanError};
use crate::project::SourceConfig;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use tracing::info;

pub struct AdbcConnector;

impl AdbcConnector {
    pub fn new() -> Self {
        Self
    }
}

impl Default for AdbcConnector {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl super::Connector for AdbcConnector {
    async fn register(
        &self,
        _ctx: &SessionContext,
        name: &str,
        config: &SourceConfig,
    ) -> Result<()> {
        info!(source = %name, "Registering ADBC source");

        let driver = config
            .driver
            .as_ref()
            .ok_or_else(|| TitanError::ValidationError("ADBC driver path required".to_string()))?;
        let uri = config
            .connection_string
            .as_ref()
            .ok_or_else(|| TitanError::ValidationError("Connection string required".to_string()))?;

        // In a real implementation with the 'adbc' crate:
        // let mut database = adbc::Database::new(driver, uri)?;
        // let mut connection = database.connect()?;
        // ... register as DataFusion TableProvider

        info!(driver = %driver, uri = %uri, "ADBC source connected (simulated for compilation)");

        Ok(())
    }
}
