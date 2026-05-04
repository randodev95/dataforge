use sov_schema_db::{define_schema, Schema, DB, schema::{KeyEncoder, KeyDecoder, ValueCodec}, CodecError};
use crate::fingerprint::LogicHash;
use serde::{Deserialize, Serialize};
use crate::error::{TitanError, Result};
use std::path::Path;
use std::fmt::Debug;
use rocksdb::Options;

define_schema!(ModelMetadataSchema, LogicHash, ModelMetadata, "model_metadata");
define_schema!(ModelValueLogSchema, LogicHash, String, "value_log");
define_schema!(NameHashIndexSchema, String, LogicHash, "name_hash_index");

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelMetadata {
    pub status: String,
    pub materialization_path: String,
    pub created_at: u64,
}

impl KeyEncoder<ModelMetadataSchema> for LogicHash {
    fn encode_key(&self) -> std::result::Result<Vec<u8>, CodecError> {
        Ok(self.as_str().as_bytes().to_vec())
    }
}
impl KeyDecoder<ModelMetadataSchema> for LogicHash {
    fn decode_key(data: &[u8]) -> std::result::Result<Self, CodecError> {
        let s = std::str::from_utf8(data).map_err(|e| CodecError::Wrapped(e.into()))?;
        Ok(LogicHash::new(s.to_string()))
    }
}

impl KeyEncoder<ModelValueLogSchema> for LogicHash {
    fn encode_key(&self) -> std::result::Result<Vec<u8>, CodecError> {
        Ok(self.as_str().as_bytes().to_vec())
    }
}
impl KeyDecoder<ModelValueLogSchema> for LogicHash {
    fn decode_key(data: &[u8]) -> std::result::Result<Self, CodecError> {
        let s = std::str::from_utf8(data).map_err(|e| CodecError::Wrapped(e.into()))?;
        Ok(LogicHash::new(s.to_string()))
    }
}

impl KeyEncoder<NameHashIndexSchema> for String {
    fn encode_key(&self) -> std::result::Result<Vec<u8>, CodecError> {
        Ok(self.as_bytes().to_vec())
    }
}
impl KeyDecoder<NameHashIndexSchema> for String {
    fn decode_key(data: &[u8]) -> std::result::Result<Self, CodecError> {
        std::str::from_utf8(data).map_err(|e| CodecError::Wrapped(e.into())).map(|s| s.to_string())
    }
}

impl ValueCodec<ModelMetadataSchema> for ModelMetadata {
    fn encode_value(&self) -> std::result::Result<Vec<u8>, CodecError> {
        serde_json::to_vec(self).map_err(|e| CodecError::Wrapped(e.into()))
    }
    fn decode_value(data: &[u8]) -> std::result::Result<Self, CodecError> {
        serde_json::from_slice(data).map_err(|e| CodecError::Wrapped(e.into()))
    }
}

impl ValueCodec<ModelValueLogSchema> for String {
    fn encode_value(&self) -> std::result::Result<Vec<u8>, CodecError> {
        Ok(self.as_bytes().to_vec())
    }
    fn decode_value(data: &[u8]) -> std::result::Result<Self, CodecError> {
        std::str::from_utf8(data).map_err(|e| CodecError::Wrapped(e.into())).map(|s| s.to_string())
    }
}

impl ValueCodec<NameHashIndexSchema> for LogicHash {
    fn encode_value(&self) -> std::result::Result<Vec<u8>, CodecError> {
        Ok(self.as_str().as_bytes().to_vec())
    }
    fn decode_value(data: &[u8]) -> std::result::Result<Self, CodecError> {
        let s = std::str::from_utf8(data).map_err(|e| CodecError::Wrapped(e.into()))?;
        Ok(LogicHash::new(s.to_string()))
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
        ).map_err(|e| crate::error::TitanError::DatabaseError(e.to_string()))?;

        Ok(Self { db })
    }

    /// Prefixes the model name with environment for isolation
    fn env_key(env: &str, name: &str) -> String {
        format!("{}::{}", env, name)
    }

    pub fn put_metadata(&self, env: &str, name: &str, hash: &LogicHash, metadata: &ModelMetadata) -> Result<()> {
        let batch = sov_schema_db::SchemaBatch::default();
        batch.put::<ModelMetadataSchema>(hash, metadata)
            .map_err(|e| TitanError::StateError(e.to_string()))?;
        batch.put::<NameHashIndexSchema>(&Self::env_key(env, name), hash)
            .map_err(|e| TitanError::StateError(e.to_string()))?;
        
        self.db.write_schemas(batch)
            .map_err(|e: anyhow::Error| TitanError::StateError(e.to_string()))?;
        Ok(())
    }

    pub fn get_metadata(&self, hash: &LogicHash) -> Result<Option<ModelMetadata>> {
        self.db.get::<ModelMetadataSchema>(hash)
            .map_err(|e| crate::error::TitanError::DatabaseError(e.to_string()))
    }

    pub fn get_hash_by_name(&self, env: &str, name: &str) -> Result<Option<LogicHash>> {
        self.db.get::<NameHashIndexSchema>(&Self::env_key(env, name))
            .map_err(|e| crate::error::TitanError::DatabaseError(e.to_string()))
    }

    pub fn put_value(&self, hash: &LogicHash, sql: String) -> Result<()> {
        self.db.put::<ModelValueLogSchema>(hash, &sql)
            .map_err(|e| crate::error::TitanError::DatabaseError(e.to_string()))
    }

    pub fn get_value(&self, hash: &LogicHash) -> Result<Option<String>> {
        self.db.get::<ModelValueLogSchema>(hash)
            .map_err(|e| crate::error::TitanError::DatabaseError(e.to_string()))
    }

    pub fn get_metadata_by_name(&self, env: &str, name: &str) -> Result<Option<ModelMetadata>> {
        if let Some(hash) = self.get_hash_by_name(env, name)? {
            self.get_metadata(&hash)
        } else {
            Ok(None)
        }
    }
}
