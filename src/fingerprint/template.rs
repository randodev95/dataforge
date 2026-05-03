use minijinja::{Environment, Value};
use std::sync::Arc;
use std::collections::HashMap;

pub struct TemplateEngine {
    env: Arc<Environment<'static>>,
}

impl TemplateEngine {
    pub fn new() -> Self {
        let mut env = Environment::new();
        
        env.set_undefined_behavior(minijinja::UndefinedBehavior::Strict);

        env.add_function("ref", Self::dbt_ref);
        env.add_function("source", Self::dbt_source);
        env.add_function("var", Self::dbt_var);
        env.add_function("is_incremental", Self::dbt_is_incremental);

        Self {
            env: Arc::new(env),
        }
    }

    pub fn render(&self, template: &str, context: &HashMap<String, Value>) -> anyhow::Result<String> {
        self.env.render_str(template, context)
            .map_err(|e| anyhow::anyhow!("Template rendering failed: {}", e))
    }

    fn dbt_ref(name: String, state: &minijinja::State) -> String {
        if let Some(env_val) = state.lookup("titan_env") {
            format!("{}_{}", env_val, name)
        } else {
            name
        }
    }

    fn dbt_source(_source_name: String, table_name: String) -> String {
        table_name
    }

    fn dbt_var(_name: String, default: Option<Value>) -> Value {
        default.unwrap_or(Value::from("placeholder_var"))
    }

    fn dbt_is_incremental() -> bool {
        false
    }
}
