use crate::error::{TitanError, Result};
use regex::Regex;
use once_cell::sync::Lazy;

static ENV_VAR_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r#"\$\{(?P<name>[^}]+)\}"#).expect("Titan: internal regex failure (ENV_VAR_RE)"));

/// Trait for resolving secrets from various backends.
pub trait SecretResolver: Send + Sync {
    fn resolve(&self, input: &str) -> Result<String>;
}

/// A resolver that substitutes environment variables in the form ${VAR_NAME}.
pub struct EnvSecretResolver;

impl SecretResolver for EnvSecretResolver {
    fn resolve(&self, input: &str) -> Result<String> {
        let mut has_unresolved = false;
        let mut last_error = None;

        let resolved = ENV_VAR_RE.replace_all(input, |caps: &regex::Captures| {
            let name = &caps["name"];
            match std::env::var(name) {
                Ok(val) => val,
                Err(_) => {
                    has_unresolved = true;
                    last_error = Some(name.to_string());
                    caps[0].to_string()
                }
            }
        });

        if has_unresolved {
            return Err(TitanError::ValidationError(format!(
                "Failed to resolve environment variable: {}", 
                last_error.unwrap_or_default()
            )));
        }

        Ok(resolved.into_owned())
    }
}

/// Masks sensitive parts of a connection string or URL for logging.
pub fn mask_secrets(input: &str) -> String {
    // Basic mask for common patterns like password=... or :password@
    let mut masked = input.to_string();
    
    let pass_re = Regex::new(r"(?i)password=[^;&\s]+").unwrap();
    masked = pass_re.replace_all(&masked, "password=******").into_owned();
    
    let url_pass_re = Regex::new(r":([^:@]+)@").unwrap();
    masked = url_pass_re.replace_all(&masked, ":******@").into_owned();

    masked
}
