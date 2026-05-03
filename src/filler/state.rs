use sov_schema_db::{define_schema, Schema, DB, schema::{KeyEncoder, KeyDecoder, ValueCodec}, CodecError};
use crate::fingerprint::LogicHash;
use serde::{Deserialize, Serialize};
use anyhow::Result;
use std::path::Path;
use std::fmt::Debug;
use rocksdb::Options;

// Define schemas for our state store
define_schema!(ModelMetadataSchema, LogicHash, ModelMetadata, "model_metadata");
define_schema!(ModelValueLogSchema, LogicHash, String, "value_log");
define_schema!(NameHashIndexSchema, String, LogicHash, "name_hash_index");

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelMetadata {
    pub status: String,
    pub materialization_path: String,
    pub created_at: u64,
}

// Implement Codecs for LogicHash as Key
impl KeyEncoder<ModelMetadataSchema> for LogicHash {
    fn encode_key(&self) -> std::result::Result<Vec<u8>, CodecError> {
        Ok(self.as_str().as_bytes().to_vec())
    }
}
impl KeyDecoder<ModelMetadataSchema> for LogicHash {
    fn decode_key(data: &[u8]) -> std::result::Result<Self, CodecError> {
        Ok(LogicHash::new(String::from_utf8(data.to_vec()).map_err(|e| CodecError::Wrapped(e.into()))?))
    }
}

impl KeyEncoder<ModelValueLogSchema> for LogicHash {
    fn encode_key(&self) -> std::result::Result<Vec<u8>, CodecError> {
        Ok(self.as_str().as_bytes().to_vec())
    }
}
impl KeyDecoder<ModelValueLogSchema> for LogicHash {
    fn decode_key(data: &[u8]) -> std::result::Result<Self, CodecError> {
        Ok(LogicHash::new(String::from_utf8(data.to_vec()).map_err(|e| CodecError::Wrapped(e.into()))?))
    }
}

// Implement Codecs for String as Key
impl KeyEncoder<NameHashIndexSchema> for String {
    fn encode_key(&self) -> std::result::Result<Vec<u8>, CodecError> {
        Ok(self.as_bytes().to_vec())
    }
}
impl KeyDecoder<NameHashIndexSchema> for String {
    fn decode_key(data: &[u8]) -> std::result::Result<Self, CodecError> {
        Ok(String::from_utf8(data.to_vec()).map_err(|e| CodecError::Wrapped(e.into()))?)
    }
}

// Implement Codecs for Values
impl ValueCodec<ModelMetadataSchema> for ModelMetadata {
    fn encode_value(&self) -> std::result::Result<Vec<u8>, CodecError> {
        Ok(serde_json::to_vec(self).map_err(|e| CodecError::Wrapped(e.into()))?)
    }
    fn decode_value(data: &[u8]) -> std::result::Result<Self, CodecError> {
        Ok(serde_json::from_slice(data).map_err(|e| CodecError::Wrapped(e.into()))?)
    }
}

impl ValueCodec<ModelValueLogSchema> for String {
    fn encode_value(&self) -> std::result::Result<Vec<u8>, CodecError> {
        Ok(self.as_bytes().to_vec())
    }
    fn decode_value(data: &[u8]) -> std::result::Result<Self, CodecError> {
        Ok(String::from_utf8(data.to_vec()).map_err(|e| CodecError::Wrapped(e.into()))?)
    }
}

impl ValueCodec<NameHashIndexSchema> for LogicHash {
    fn encode_value(&self) -> std::result::Result<Vec<u8>, CodecError> {
        Ok(self.as_str().as_bytes().to_vec())
    }
    fn decode_value(data: &[u8]) -> std::result::Result<Self, CodecError> {
        Ok(LogicHash::new(String::from_utf8(data.to_vec()).map_err(|e| CodecError::Wrapped(e.into()))?))
    }
}

pub struct StateStore {
    db: DB,
}

impl StateStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        let db = DB::open(
            path,
            "titan_state",
            vec![
                ModelMetadataSchema::COLUMN_FAMILY_NAME,
                ModelValueLogSchema::COLUMN_FAMILY_NAME,
                NameHashIndexSchema::COLUMN_FAMILY_NAME,
            ],
            &options,
        ).map_err(|e| anyhow::anyhow!("Failed to open RocksDB: {}", e))?;

        Ok(Self { db })
    }

    /// Prefixes the model name with environment for isolation
    fn env_key(env: &str, name: &str) -> String {
        format!("{}::{}", env, name)
    }

    pub fn put_metadata(&self, env: &str, name: &str, hash: &LogicHash, metadata: &ModelMetadata) -> Result<()> {
        self.db.put::<ModelMetadataSchema>(hash, metadata)?;
        self.db.put::<NameHashIndexSchema>(&Self::env_key(env, name), hash)?;
        Ok(())
    }

    pub fn get_metadata(&self, hash: &LogicHash) -> Result<Option<ModelMetadata>> {
        self.db.get::<ModelMetadataSchema>(hash)
            .map_err(|e| anyhow::anyhow!("Failed to get metadata: {}", e))
    }

    pub fn get_hash_by_name(&self, env: &str, name: &str) -> Result<Option<LogicHash>> {
        self.db.get::<NameHashIndexSchema>(&Self::env_key(env, name))
            .map_err(|e| anyhow::anyhow!("Failed to get hash by name: {}", e))
    }

    pub fn put_value(&self, hash: &LogicHash, sql: String) -> Result<()> {
        self.db.put::<ModelValueLogSchema>(hash, &sql)
            .map_err(|e| anyhow::anyhow!("Failed to put value: {}", e))
    }

    pub fn get_value(&self, hash: &LogicHash) -> Result<Option<String>> {
        self.db.get::<ModelValueLogSchema>(hash)
            .map_err(|e| anyhow::anyhow!("Failed to get value: {}", e))
    }
}
