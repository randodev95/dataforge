//! # Graceful Column Deletion
//!
//! Tracks columns that have been removed from the source SQL but should
//! persist in the warehouse for a grace period (NULL-filled).

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GracePeriodRecord {
    pub model_name: String,
    pub column_name: String,
    pub data_type: String,
    pub first_seen_dropped_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

pub struct GracePeriodManager;

impl GracePeriodManager {
    pub fn compute_drops(
        model_name: &str,
        dropped_columns: &[String],
        existing_records: &[GracePeriodRecord],
        grace_period_days: i64,
        now: DateTime<Utc>,
    ) -> GracePeriodResult {
        let mut new_records = Vec::new();
        let mut still_in_grace = Vec::new();
        let mut expired = Vec::new();

        let existing_map: std::collections::HashMap<String, &GracePeriodRecord> = existing_records
            .iter()
            .map(|r| (r.column_name.to_lowercase(), r))
            .collect();

        let dropped_set: std::collections::HashSet<String> =
            dropped_columns.iter().map(|c| c.to_lowercase()).collect();

        // 1. Process current dropped columns
        for col_name in dropped_columns {
            if let Some(record) = existing_map.get(&col_name.to_lowercase()) {
                if now >= record.expires_at {
                    expired.push(col_name.clone());
                } else {
                    still_in_grace.push((*record).clone());
                }
            } else {
                // New drop
                let expires_at = now + Duration::days(grace_period_days);
                new_records.push(GracePeriodRecord {
                    model_name: model_name.to_string(),
                    column_name: col_name.clone(),
                    data_type: "UNKNOWN".to_string(), // Should be filled from target schema if possible
                    first_seen_dropped_at: now,
                    expires_at,
                });
            }
        }

        // 2. Detect reappeared columns (were in grace period but now back in SQL)
        let reappeared: Vec<String> = existing_records
            .iter()
            .filter(|r| !dropped_set.contains(&r.column_name.to_lowercase()))
            .map(|r| r.column_name.clone())
            .collect();

        GracePeriodResult {
            new_records,
            still_in_grace,
            expired,
            reappeared,
        }
    }
}

pub struct GracePeriodResult {
    pub new_records: Vec<GracePeriodRecord>,
    pub still_in_grace: Vec<GracePeriodRecord>,
    pub expired: Vec<String>,
    pub reappeared: Vec<String>,
}
