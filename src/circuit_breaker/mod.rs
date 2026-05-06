//! Circuit breaker pattern for pipeline reliability.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use thiserror::Error;

#[derive(Debug, Error)]
#[error("circuit breaker tripped after {consecutive_failures} failures — last error: {last_error}")]
pub struct CircuitBreakerTripped {
    pub consecutive_failures: u32,
    pub last_error: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Closed,
    Open,
    HalfOpen,
}

/// Thread-safe circuit breaker.
pub struct CircuitBreaker {
    consecutive_failures: AtomicU32,
    threshold: u32,
    state: AtomicU32, // 0: Closed, 1: Open, 2: HalfOpen
    tripped_at_ms: AtomicU64,
    recovery_timeout: Duration,
    epoch: Instant,
}

impl CircuitBreaker {
    pub fn new(threshold: u32, recovery_timeout: Duration) -> Self {
        Self {
            consecutive_failures: AtomicU32::new(0),
            threshold,
            state: AtomicU32::new(0),
            tripped_at_ms: AtomicU64::new(0),
            recovery_timeout,
            epoch: Instant::now(),
        }
    }

    pub fn check(&self) -> Result<(), CircuitBreakerTripped> {
        let state = self.state.load(Ordering::Acquire);
        if state == 0 || state == 2 {
            return Ok(());
        }

        let tripped_at = self.tripped_at_ms.load(Ordering::Acquire);
        let elapsed = self.epoch.elapsed().as_millis() as u64;

        if elapsed.saturating_sub(tripped_at) >= self.recovery_timeout.as_millis() as u64 {
            // Transition to HalfOpen
            self.state.store(2, Ordering::Release);
            return Ok(());
        }

        Err(CircuitBreakerTripped {
            consecutive_failures: self.consecutive_failures.load(Ordering::Relaxed),
            last_error: "Warehouse unavailable".to_string(),
        })
    }

    pub fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::Release);
        self.state.store(0, Ordering::Release);
    }

    pub fn record_failure(&self) {
        let count = self.consecutive_failures.fetch_add(1, Ordering::AcqRel) + 1;
        if count >= self.threshold {
            self.state.store(1, Ordering::Release);
            self.tripped_at_ms
                .store(self.epoch.elapsed().as_millis() as u64, Ordering::Release);
        }
    }
}
