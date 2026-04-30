use wasmtime::*;
use crate::error::Result;

pub struct WasmRuntime {
    pub engine: Engine,
    pub store: Store<()>,
}

impl WasmRuntime {
    pub fn new() -> Result<Self> {
        let engine = Engine::default();
        let store = Store::new(&engine, ());
        Ok(Self { engine, store })
    }

    pub fn load_plugin(&mut self, path: &str) -> Result<Instance> {
        let module = Module::from_file(&self.engine, path)
            .map_err(|e: anyhow::Error| crate::error::DataForgeError::Other(e))?;
        
        let linker = Linker::new(&self.engine);
        let instance = linker.instantiate(&mut self.store, &module)
            .map_err(|e: anyhow::Error| crate::error::DataForgeError::Other(e))?;
            
        Ok(instance)
    }

    pub fn run_validation(&mut self, instance: Instance, amount: f64) -> Result<bool> {
        let func = instance.get_typed_func::<f64, i32>(&mut self.store, "validate_model")
            .map_err(|e: anyhow::Error| crate::error::DataForgeError::Other(e))?;
            
        let res = func.call(&mut self.store, amount)
            .map_err(|e: anyhow::Error| crate::error::DataForgeError::Other(e))?;
            
        Ok(res == 1)
    }
}
