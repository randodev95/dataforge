use crate::error::Result;
use std::path::PathBuf;
use std::process::Command;

pub struct SdfBridge {
    pub root: PathBuf,
}

impl SdfBridge {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub fn compile(&self) -> Result<()> {
        if std::env::var("DATAFORGE_MOCK_SDF").is_ok() {
            println!("SDF: (MOCK) Compiling workspace");
            return Ok(());
        }
        
        println!("SDF: Compiling workspace at {:?}", self.root);
        let status = Command::new("sdf")
            .arg("compile")
            .current_dir(&self.root)
            .status()
            .map_err(|e| crate::error::DataForgeError::Other(e.into()))?;

        if !status.success() {
            return Err(crate::error::DataForgeError::Other(anyhow::anyhow!("SDF compilation failed")));
        }
        Ok(())
    }

    pub fn get_column_lineage(&self, _model: &str) -> Result<Vec<String>> {
        // TODO: Parse .sdf/information_schema.db or similar
        Ok(vec!["parent_table.id".into(), "parent_table.created_at".into()])
    }
}
