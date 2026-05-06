use crate::execution::Muscle;
use tracing::debug;

/// RAII guard for DataFusion table registrations.
///
/// Ensures that a table registered in the SessionContext is automatically
/// deregistered when the guard is dropped.
pub struct TableGuard {
    muscle: std::sync::Arc<Muscle>,
    name: String,
}

impl TableGuard {
    /// Creates a new TableGuard. Does NOT register the table itself.
    /// The user should register the table after creating the guard.
    pub fn new(muscle: std::sync::Arc<Muscle>, name: &str) -> Self {
        Self {
            muscle,
            name: name.to_string(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Drop for TableGuard {
    fn drop(&mut self) {
        debug!(table = %self.name, "Deregistering table via guard");
        if let Err(e) = self.muscle.ctx.deregister_table(&self.name) {
            // We only log an error if the table actually existed and failed to deregister.
            // If it was already deregistered or never registered, DataFusion might return an error
            // depending on the version/context, but we should be safe to ignore it if we're just cleaning up.
            debug!(table = %self.name, error = %e, "Failed to deregister table (might have been already removed)");
        }
    }
}
