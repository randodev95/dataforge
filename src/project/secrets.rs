use crate::error::{Result, TitanError};
use regex::Regex;
use std::sync::LazyLock;

static ENV_VAR_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\$\{(?P<name>[^}]+)\}").expect("Titan: internal regex failure (ENV_VAR_RE)")
});

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
            if let Ok(val) = std::env::var(name) {
                val
            } else {
                has_unresolved = true;
                last_error = Some(name.to_string());
                caps[0].to_string()
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

static PASS_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)password=[^;&\s]+").expect("Titan: internal regex failure (PASS_RE)")
});
static URL_PASS_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r":([^:@]+)@").expect("Titan: internal regex failure (URL_PASS_RE)")
});

/// Masks sensitive parts of a connection string or URL for logging.
pub fn mask_secrets(input: &str) -> String {
    let masked = PASS_RE.replace_all(input, "password=******");
    URL_PASS_RE.replace_all(&masked, ":******@").into_owned()
}
