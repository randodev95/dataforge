use minijinja::{Environment, Value, context};
use std::sync::Arc;
use std::collections::HashMap;
use std::path::Path;

pub struct TemplateEngine {
    env: Arc<Environment<'static>>,
}

impl TemplateEngine {
    pub fn new(project_root: &Path) -> Self {
        let mut env = Environment::new();
        
        env.set_undefined_behavior(minijinja::UndefinedBehavior::Strict);

        // 1. Setup Macro Loader
        let macros_dir = project_root.join("macros");
        if macros_dir.exists() {
            env.set_loader(minijinja::path_loader(macros_dir));
        }

        // 2. Global Functions
        env.add_function("ref", Self::dbt_ref);
        env.add_function("source", Self::dbt_source);
        env.add_function("var", Self::dbt_var);
        env.add_function("env_var", Self::dbt_env_var);
        env.add_function("config", Self::dbt_config);
        env.add_function("is_incremental", Self::dbt_is_incremental);

        Self {
            env: Arc::new(env),
        }
    }

    pub fn render(
        &self, 
        template: &str, 
        ctx_vals: &HashMap<String, Value>,
        this_model: &str,
        target_name: &str,
        is_inc: bool
    ) -> anyhow::Result<String> {
        let ctx = context! {
            this => context! { name => this_model },
            target => context! { name => target_name },
            is_incremental_val => is_inc,
            ..Value::from(ctx_vals.clone())
        };

        self.env.render_str(template, ctx)
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

    fn dbt_var(name: String, default: Option<Value>, state: &minijinja::State) -> Value {
        if let Some(vars) = state.lookup("vars") {
            if let Some(val) = vars.get_attr(&name).ok() {
                return val;
            }
        }
        default.unwrap_or(Value::from("placeholder_var"))
    }

    fn dbt_env_var(name: String, default: Option<String>) -> String {
        std::env::var(name).unwrap_or_else(|_| default.unwrap_or_default())
    }

    fn dbt_is_incremental(state: &minijinja::State) -> bool {
        state.lookup("is_incremental_val")
            .map(|v| v.is_true())
            .unwrap_or(false)
    }

    fn dbt_config(#[allow(unused_variables)] kwargs: minijinja::value::Rest<minijinja::Value>) -> String {
        "".to_string()
    }
}
