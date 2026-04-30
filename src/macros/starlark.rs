use starlark::environment::{Globals, GlobalsBuilder};
use starlark::eval::Evaluator;
use starlark::syntax::{AstModule, Dialect};
use starlark::starlark_module;
use starlark::values::{Value, list_or_tuple::UnpackListOrTuple};
use std::sync::Arc;
use crate::error::Result;

pub struct StarlarkEngine {
    globals: Globals,
}

#[starlark_module]
fn dataforge_builtins(builder: &mut GlobalsBuilder) {
    fn r#ref(name: &str) -> anyhow::Result<String> {
        Ok(name.to_string())
    }

    fn source(source_name: &str, table_name: &str) -> anyhow::Result<String> {
        Ok(format!("{}.{}", source_name, table_name))
    }
}

impl StarlarkEngine {
    pub fn new() -> Self {
        let globals = GlobalsBuilder::new()
            .with(dataforge_builtins)
            .build();
        Self { globals }
    }

    pub fn expand_sql(&self, sql: &str) -> Result<String> {
        let re = regex::Regex::new(r"\$\{(?P<code>[^}]+)\}").unwrap();
        let mut result = sql.to_string();
        
        let matches: Vec<_> = re.captures_iter(sql).collect();
        for cap in matches.iter().rev() {
            let expanded = self.expand_expr(&cap["code"])?;
            result.replace_range(cap.get(0).unwrap().range(), &expanded);
        }
        
        Ok(result)
    }

    fn expand_expr(&self, expr: &str) -> Result<String> {
        let module = starlark::environment::Module::new();
        let mut eval = Evaluator::new(&module);
        
        let ast = AstModule::parse("expr.star", expr.to_string(), &Dialect::Standard)
            .map_err(|e| crate::error::DataForgeError::StarlarkError(e.to_string()))?;
            
        let res = eval.eval_module(ast, &self.globals)
            .map_err(|e| crate::error::DataForgeError::StarlarkError(e.to_string()))?;
            
        Ok(res.to_str().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_expansion() {
        let engine = StarlarkEngine::new();
        let sql = "SELECT * FROM ${ref('orders')} WHERE date > '2023-01-01'";
        let result = engine.expand_sql(sql).unwrap();
        assert_eq!(result, "SELECT * FROM orders WHERE date > '2023-01-01'");
    }
}
