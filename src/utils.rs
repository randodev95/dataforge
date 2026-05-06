//! # Shared Utilities
//!
//! This module provides shared helper functions for performance timing,
//! SQL construction, and other common cross-cutting concerns.

/// Efficient SQL query builder with pre-allocated buffer.
pub struct SqlBuilder {
    inner: String,
}

impl SqlBuilder {
    #[inline]
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: String::with_capacity(capacity),
        }
    }

    #[inline]
    pub fn push_str(&mut self, s: &str) {
        self.inner.push_str(s);
    }

    #[inline]
    pub fn finish(self) -> String {
        self.inner
    }
}

#[inline]
pub fn measure_duration<F, R>(f: F) -> (R, u128)
where
    F: FnOnce() -> R,
{
    let start = std::time::Instant::now();
    let result = f();
    (result, start.elapsed().as_millis())
}
#[inline]
pub fn quote_identifier(name: &str) -> String {
    use sqlparser::ast::Ident;
    Ident::with_quote('"', name).to_string()
}
