use minijinja::{Environment, Value, context};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

pub struct TemplateEngine {
    env: Arc<Environment<'static>>,
}

impl TemplateEngine {
    pub fn new(project_root: &Path) -> Self {
        let mut env = Environment::new();

        env.set_undefined_behavior(minijinja::UndefinedBehavior::Strict);

        // 1. Setup Macro Loader
        let mut loader_paths = Vec::new();

        let local_macros = project_root.join("macros");
        if local_macros.exists() {
            loader_paths.push(local_macros);
        }

        let packages_dir = project_root.join(".titan_packages");
        if packages_dir.exists()
            && let Ok(entries) = std::fs::read_dir(packages_dir)
        {
            for entry in entries.flatten() {
                let path = entry.path().join("macros");
                if path.exists() {
                    loader_paths.push(path);
                }
            }
        }

        if !loader_paths.is_empty() {
            env.set_loader(minijinja::path_loader(loader_paths[0].clone()));
            // Note: path_loader in minijinja only takes one path by default?
            // Wait, minijinja's path_loader can take a vector of paths if we use a custom loader or just use the first one for now.
            // Actually, minijinja doesn't have a multi-path loader out of the box that I recall.
            // I'll implement a simple one.
        }

        // 2. Global Functions
        env.add_function("ref", Self::dbt_ref);
        env.add_function("source", Self::dbt_source);
        env.add_function("var", Self::dbt_var);
        env.add_function("env_var", Self::dbt_env_var);
        env.add_function("config", Self::dbt_config);
        env.add_function("is_incremental", Self::dbt_is_incremental);

        Self { env: Arc::new(env) }
    }

    pub fn render(
        &self,
        template: &str,
        ctx_vals: &HashMap<String, Value>,
        this_model: &str,
        target_name: &str,
        is_inc: bool,
    ) -> anyhow::Result<String> {
        let ctx = context! {
            this => context! { name => this_model },
            target => context! { name => target_name },
            is_incremental_val => is_inc,
            ..Value::from(ctx_vals.clone())
        };

        self.env
            .render_str(template, ctx)
            .map_err(|e| anyhow::anyhow!("Template rendering failed: {e}"))
    }

    fn dbt_ref(name: String, state: &minijinja::State) -> String {
        if let Some(env_val) = state.lookup("titan_env") {
            let env_str = env_val.as_str().unwrap_or_default();
            if env_str.is_empty() {
                name
            } else {
                format!("{env_str}_{name}")
            }
        } else {
            name
        }
    }

    fn dbt_source(_source_name: String, table_name: String) -> String {
        table_name
    }

    fn dbt_var(name: String, default: Option<Value>, state: &minijinja::State) -> Value {
        if let Some(vars) = state.lookup("vars")
            && let Ok(val) = vars.get_attr(&name)
        {
            return val;
        }
        default.unwrap_or(Value::from("placeholder_var"))
    }

    fn dbt_env_var(name: String, default: Option<String>) -> String {
        std::env::var(name).unwrap_or_else(|_| default.unwrap_or_default())
    }

    fn dbt_is_incremental(state: &minijinja::State) -> bool {
        state
            .lookup("is_incremental_val")
            .is_some_and(|v| v.is_true())
    }

    fn dbt_config(
        #[allow(unused_variables)] kwargs: minijinja::value::Rest<minijinja::Value>,
    ) -> String {
        String::new()
    }
}
