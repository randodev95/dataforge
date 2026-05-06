//! Lightweight string interning for repeated identifiers.
//!
//! Inspired by Rocky RS and backed by `lasso::ThreadedRodeo`.

use lasso::{Spur, ThreadedRodeo};

/// A thread-safe string interner.
pub struct Interner {
    inner: ThreadedRodeo,
}

/// An interned string handle.
pub type InternKey = Spur;

impl Interner {
    pub fn new() -> Self {
        Self {
            inner: ThreadedRodeo::new(),
        }
    }

    /// Intern a string, returning a key.
    pub fn intern(&self, s: &str) -> InternKey {
        self.inner.get_or_intern(s)
    }

    /// Resolve an interned key back to its string.
    pub fn resolve(&self, key: InternKey) -> &str {
        self.inner.resolve(&key)
    }
}

impl Default for Interner {
    fn default() -> Self {
        Self::new()
    }
}

/// Global interner for the entire application.
pub static GLOBAL_INTERNER: std::sync::LazyLock<Interner> = std::sync::LazyLock::new(Interner::new);

/// Helper to intern a string using the global interner.
pub fn intern(s: &str) -> InternKey {
    GLOBAL_INTERNER.intern(s)
}

/// Helper to resolve a key using the global interner.
pub fn resolve(key: InternKey) -> &'static str {
    // Note: lasso's resolve returns a &str with the lifetime of the interner.
    // Since GLOBAL_INTERNER is static, we can safely cast to 'static.
    unsafe { std::mem::transmute(GLOBAL_INTERNER.resolve(key)) }
}
