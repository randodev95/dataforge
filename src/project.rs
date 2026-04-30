use std::path::{Path, PathBuf};
use std::fs;
use crate::error::Result;
use walkdir::WalkDir;

pub struct Project {
    pub root: PathBuf,
}

impl Project {
    pub fn init(path: &Path) -> Result<Self> {
        fs::create_dir_all(path.join("models/bronze"))?;
        fs::create_dir_all(path.join("models/silver"))?;
        fs::create_dir_all(path.join("models/gold"))?;
        fs::create_dir_all(path.join("macros"))?;
        fs::write(path.join("dataforge.yaml"), "project: DataForge\n")?;
        Ok(Self { root: path.to_path_buf() })
    }

    pub fn load(path: &Path) -> Result<Self> {
        if !path.join("dataforge.yaml").exists() {
            return Err(crate::error::DataForgeError::EnvNotFound("Project root missing dataforge.yaml".into()));
        }
        Ok(Self { root: path.to_path_buf() })
    }

    pub fn discover_models(&self) -> Vec<PathBuf> {
        WalkDir::new(self.root.join("models"))
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "sql"))
            .map(|e| e.path().to_path_buf())
            .collect()
    }

    pub fn discover_macros(&self) -> Vec<PathBuf> {
        WalkDir::new(self.root.join("macros"))
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "stark"))
            .map(|e| e.path().to_path_buf())
            .collect()
    }
}
