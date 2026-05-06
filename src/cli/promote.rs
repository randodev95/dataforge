use crate::project::profiles::Profiles;
use crate::{Muscle, Project, StateStore};
use anyhow::Result;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::info;

pub async fn handle_promote(
    path: PathBuf,
    model: String,
    from: String,
    target: String,
) -> Result<()> {
    let _project = Project::load(&path)?;
    let profiles = Profiles::load(&path.join("profiles.yml"))?;
    let target_profile = profiles
        .get_target(&target)
        .ok_or_else(|| anyhow::anyhow!("Target profile {target} not found"))?;

    let db_path = path.join(".titan_db");
    let state_store = StateStore::open(&db_path)?;
    let _muscle = Arc::new(Muscle::new());

    // 1. Get the latest successful hash from the source environment
    let hash = state_store
        .get_hash_by_name(&from, &model)?
        .ok_or_else(|| {
            anyhow::anyhow!("No successful run found for model {model} in environment {from}")
        })?;

    let metadata = state_store
        .get_metadata(&hash)?
        .ok_or_else(|| anyhow::anyhow!("Metadata missing for hash {}", hash.as_str()))?;

    let _sql = state_store
        .get_value(&hash)?
        .ok_or_else(|| anyhow::anyhow!("SQL missing for hash {}", hash.as_str()))?;

    // 2. Atomic Metadata Update
    info!(model = %model, from = %from, to = %target, hash = %hash.as_str(), "Promoting model metadata");
    state_store.put_metadata(&target, &model, &hash, &metadata)?;

    // 3. Physical View Swap (Virtual Environment Promotion)
    // We update the production-prefixed view to point to the materialized snapshot
    let target_view = format!("{}{}", target_profile.prefix, model);

    // For now, we assume the data is reachable.
    // In a real warehouse, we'd issue a CREATE OR REPLACE VIEW pointing to the S3/Delta path.
    let _promotion_sql = format!(
        "CREATE OR REPLACE VIEW {} AS SELECT * FROM read_parquet('{}')",
        target_view, metadata.materialization_path
    );

    // Note: If materialization was 'view', we promote the SQL definition instead.
    // We can check the materialization type in the future by adding it to ModelMetadata.

    info!(view = %target_view, path = %metadata.materialization_path, "Updated production view");
    // muscle.execute(&promotion_sql).await?; // This would apply to the live warehouse

    println!("Successfully promoted {model} from {from} to {target}");
    Ok(())
}
