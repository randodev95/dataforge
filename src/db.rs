use sqlx::{Pool, Postgres, Sqlite, Row};
use std::collections::HashMap;
use crate::error::{DataForgeError, Result};
use crate::types::{Model, ModelName, EnvName};
use crate::{StateStore, WarehouseConnector};
use std::sync::{Arc, Mutex};
use async_trait::async_trait;
use futures::StreamExt;

pub struct WarehouseConfig {
    pub connection_url: String,
    pub pool_size: u32,
    pub timeout_seconds: u32,
}

impl WarehouseConfig {
    pub fn builder() -> WarehouseConfigBuilder {
        WarehouseConfigBuilder::default()
    }
}

#[derive(Default)]
pub struct WarehouseConfigBuilder {
    url: Option<String>,
    pool_size: Option<u32>,
    timeout: Option<u32>,
}

impl WarehouseConfigBuilder {
    pub fn url(mut self, url: &str) -> Self {
        self.url = Some(url.to_string());
        self
    }
    pub fn pool_size(mut self, size: u32) -> Self {
        self.pool_size = Some(size);
        self
    }
    pub fn build(self) -> Result<WarehouseConfig> {
        Ok(WarehouseConfig {
            connection_url: self.url.ok_or_else(|| DataForgeError::Other(anyhow::anyhow!("URL required")))?,
            pool_size: self.pool_size.unwrap_or(10),
            timeout_seconds: self.timeout.unwrap_or(30),
        })
    }
}

pub struct PostgresStore {
    pool: Pool<Postgres>,
}

impl PostgresStore {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    pub async fn init(&self) -> Result<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS dataforge_models (
                env TEXT NOT NULL,
                name TEXT NOT NULL,
                metadata JSONB NOT NULL,
                PRIMARY KEY (env, name)
            )"
        )
        .execute(&self.pool)
        .await
        .map_err(|e| DataForgeError::StorageError(e.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl StateStore for PostgresStore {
    async fn save_models(&self, env: &EnvName, models: &HashMap<ModelName, Model>) -> Result<()> {
        let mut tx = self.pool.begin().await.map_err(|e| DataForgeError::StorageError(e.to_string()))?;
        for (name, model) in models {
            let metadata = serde_json::to_value(model).map_err(|e| DataForgeError::StorageError(e.to_string()))?;
            sqlx::query(
                "INSERT INTO dataforge_models (env, name, metadata)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (env, name) DO UPDATE SET metadata = $3"
            )
            .bind(&env.0)
            .bind(&name.0)
            .bind(metadata)
            .execute(&mut *tx)
            .await
            .map_err(|e| DataForgeError::StorageError(e.to_string()))?;
        }
        tx.commit().await.map_err(|e| DataForgeError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn load_models(&self, env: &EnvName) -> Result<HashMap<ModelName, Model>> {
        let mut stream = sqlx::query(
            "SELECT name, metadata FROM dataforge_models WHERE env = $1"
        )
        .bind(&env.0)
        .fetch(&self.pool);

        let mut models = HashMap::new();
        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| DataForgeError::StorageError(e.to_string()))?;
            let name: String = row.try_get("name").map_err(|e| DataForgeError::StorageError(e.to_string()))?;
            let metadata: serde_json::Value = row.try_get("metadata").map_err(|e| DataForgeError::StorageError(e.to_string()))?;
            let model: Model = serde_json::from_value(metadata).map_err(|e| DataForgeError::StorageError(e.to_string()))?;
            models.insert(ModelName(name), model);
        }
        Ok(models)
    }
}

pub struct SqliteStore {
    pool: Pool<Sqlite>,
}

impl SqliteStore {
    pub fn new(pool: Pool<Sqlite>) -> Self {
        Self { pool }
    }

    pub async fn init(&self) -> Result<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS dataforge_models (
                env TEXT NOT NULL,
                name TEXT NOT NULL,
                metadata TEXT NOT NULL,
                PRIMARY KEY (env, name)
            )"
        )
        .execute(&self.pool)
        .await
        .map_err(|e| DataForgeError::StorageError(e.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl StateStore for SqliteStore {
    async fn save_models(&self, env: &EnvName, models: &HashMap<ModelName, Model>) -> Result<()> {
        let mut tx = self.pool.begin().await.map_err(|e| DataForgeError::StorageError(e.to_string()))?;
        for (name, model) in models {
            let metadata = serde_json::to_string(model).map_err(|e| DataForgeError::StorageError(e.to_string()))?;
            sqlx::query(
                "INSERT INTO dataforge_models (env, name, metadata)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (env, name) DO UPDATE SET metadata = $3"
            )
            .bind(&env.0)
            .bind(&name.0)
            .bind(metadata)
            .execute(&mut *tx)
            .await
            .map_err(|e| DataForgeError::StorageError(e.to_string()))?;
        }
        tx.commit().await.map_err(|e| DataForgeError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn load_models(&self, env: &EnvName) -> Result<HashMap<ModelName, Model>> {
        let mut stream = sqlx::query(
            "SELECT name, metadata FROM dataforge_models WHERE env = $1"
        )
        .bind(&env.0)
        .fetch(&self.pool);

        let mut models = HashMap::new();
        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| DataForgeError::StorageError(e.to_string()))?;
            let name: String = row.try_get("name").map_err(|e| DataForgeError::StorageError(e.to_string()))?;
            let metadata_str: String = row.try_get("metadata").map_err(|e| DataForgeError::StorageError(e.to_string()))?;
            let model: Model = serde_json::from_str(&metadata_str).map_err(|e| DataForgeError::StorageError(e.to_string()))?;
            models.insert(ModelName(name), model);
        }
        Ok(models)
    }
}

pub struct DuckDBConnector {
    conn: Arc<Mutex<duckdb::Connection>>,
}

impl DuckDBConnector {
    pub fn new(conn: duckdb::Connection) -> Self {
        Self { conn: Arc::new(Mutex::new(conn)) }
    }
}

#[async_trait]
impl WarehouseConnector for DuckDBConnector {
    async fn execute(&self, sql: &str) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let sql_owned = sql.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(&sql_owned, [])
        }).await.map_err(|e| DataForgeError::Other(e.into()))?
        .map_err(|e: duckdb::Error| DataForgeError::WarehouseError(e.to_string()))?;
        Ok(())
    }

    async fn fetch_columns(&self, table: &str) -> Result<Vec<String>> {
        let conn = Arc::clone(&self.conn);
        let table_owned = table.to_string();
        tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<String>> {
            let conn = conn.lock().unwrap();
            let mut stmt = conn.prepare(&format!("PRAGMA table_info('{}')", table_owned))?;
            let mut rows = stmt.query([])?;
            let mut cols = vec![];
            while let Some(row) = rows.next()? {
                let name: String = row.get("name")?;
                cols.push(name);
            }
            Ok(cols)
        }).await.map_err(|e| DataForgeError::Other(e.into()))?
        .map_err(|e: anyhow::Error| DataForgeError::WarehouseError(e.to_string()))
    }

    async fn estimate_cost(&self, _sql: &str) -> Result<f64> {
        // DuckDB is local, cost is effectively zero or simple CPU cycles
        Ok(0.01)
    }
}

pub struct PostgresConnector {
    pool: Pool<Postgres>,
}

impl PostgresConnector {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl WarehouseConnector for PostgresConnector {
    async fn execute(&self, sql: &str) -> Result<()> {
        sqlx::query(sql).execute(&self.pool).await
            .map_err(|e| DataForgeError::WarehouseError(e.to_string()))?;
        Ok(())
    }

    async fn fetch_columns(&self, table: &str) -> Result<Vec<String>> {
        let rows = sqlx::query(
            "SELECT column_name FROM information_schema.columns WHERE table_name = $1"
        )
        .bind(table)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DataForgeError::WarehouseError(e.to_string()))?;
        Ok(rows.into_iter().map(|r| r.get(0)).collect())
    }

    async fn estimate_cost(&self, _sql: &str) -> Result<f64> {
        // Mock cost for Postgres
        Ok(1.0)
    }
}

pub struct SqliteConnector {
    pool: Pool<Sqlite>,
}

impl SqliteConnector {
    pub fn new(pool: Pool<Sqlite>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl WarehouseConnector for SqliteConnector {
    async fn execute(&self, sql: &str) -> Result<()> {
        sqlx::query(sql).execute(&self.pool).await
            .map_err(|e| DataForgeError::WarehouseError(e.to_string()))?;
        Ok(())
    }

    async fn fetch_columns(&self, table: &str) -> Result<Vec<String>> {
        let rows = sqlx::query(&format!("PRAGMA table_info('{}')", table))
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DataForgeError::WarehouseError(e.to_string()))?;
        Ok(rows.into_iter().map(|r| r.get("name")).collect())
    }

    async fn estimate_cost(&self, _sql: &str) -> Result<f64> {
        Ok(0.05)
    }
}
