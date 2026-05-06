use crate::project::profiles::Profiles;
use crate::{Project, StateStore};
use anyhow::Result;
use std::path::PathBuf;

pub fn handle_status(path: PathBuf, target: String) -> Result<()> {
    let project = Project::load(&path)?;
    let profiles = Profiles::load(&path.join("profiles.yml"))?;
    let profile = profiles
        .get_target(&target)
        .ok_or_else(|| anyhow::anyhow!("Target profile {target} not found"))?;

    let db_path = path.join(".titan_db");
    let state_store = StateStore::open(&db_path)?;

    println!("Titan Project Status: {}", project.config.name);
    println!("Environment: {} (Prefix: {})", target, profile.prefix);
    println!(
        "{:<30} {:<15} {:<15} {:<10}",
        "Model", "Status", "Logic Hash", "Last Run"
    );
    println!("{}", "-".repeat(80));

    for model in &project.models {
        let hash_opt = state_store.get_hash_by_name(&target, &model.name)?;
        match hash_opt {
            Some(hash) => {
                let meta = state_store.get_metadata(&hash)?;
                match meta {
                    Some(m) => {
                        let last_run = chrono::DateTime::from_timestamp(m.created_at as i64, 0)
                            .map_or_else(
                                || "N/A".to_string(),
                                |dt| dt.format("%Y-%m-%d %H:%M").to_string(),
                            );

                        println!(
                            "{:<30} {:<15} {:<15} {:<10}",
                            model.name,
                            m.status,
                            &hash.as_str()[..8],
                            last_run
                        );
                    }
                    None => {
                        println!(
                            "{:<30} {:<15} {:<15} {:<10}",
                            model.name,
                            "MISSING_META",
                            &hash.as_str()[..8],
                            "N/A"
                        );
                    }
                }
            }
            None => {
                println!(
                    "{:<30} {:<15} {:<15} {:<10}",
                    model.name, "NOT_RUN", "N/A", "N/A"
                );
            }
        }
    }

    Ok(())
}
