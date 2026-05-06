use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use tracing::info;

#[derive(Debug, Serialize, Deserialize)]
pub struct Package {
    pub git: String,
    pub name: String,
    pub revision: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PackagesConfig {
    pub packages: Vec<Package>,
}

pub fn handle_deps(path: PathBuf) -> Result<()> {
    let packages_file = path.join("packages.yml");
    if !packages_file.exists() {
        info!("No packages.yml found, skipping dependency installation");
        return Ok(());
    }

    let content = fs::read_to_string(&packages_file)?;
    let config: PackagesConfig = serde_yml::from_str(&content)?;

    let packages_dir = path.join(".titan_packages");
    if !packages_dir.exists() {
        fs::create_dir_all(&packages_dir)?;
    }

    for pkg in config.packages {
        let pkg_path = packages_dir.join(&pkg.name);
        if pkg_path.exists() {
            info!(package = %pkg.name, "Updating package");
            let status = Command::new("git")
                .arg("-C")
                .arg(&pkg_path)
                .arg("pull")
                .status()?;
            if !status.success() {
                return Err(anyhow::anyhow!("Failed to update package {}", pkg.name));
            }
        } else {
            info!(package = %pkg.name, git = %pkg.git, "Cloning package");
            let status = Command::new("git")
                .arg("clone")
                .arg(&pkg.git)
                .arg(&pkg_path)
                .status()?;
            if !status.success() {
                return Err(anyhow::anyhow!("Failed to clone package {}", pkg.name));
            }
        }

        if let Some(rev) = &pkg.revision {
            info!(package = %pkg.name, revision = %rev, "Checking out revision");
            let status = Command::new("git")
                .arg("-C")
                .arg(&pkg_path)
                .arg("checkout")
                .arg(rev)
                .status()?;
            if !status.success() {
                return Err(anyhow::anyhow!(
                    "Failed to checkout revision {} for package {}",
                    rev,
                    pkg.name
                ));
            }
        }
    }

    println!("Successfully installed dependencies");
    Ok(())
}
