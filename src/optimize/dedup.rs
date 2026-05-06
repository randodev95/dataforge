//! # Deduplication Analysis
//!
//! This module analyzes partition checksums to find redundant data across tables.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedupStats {
    pub total_partitions: usize,
    pub unique_partitions: usize,
    pub duplicate_partitions: usize,
    pub total_rows: u64,
    pub estimated_savings_pct: f64,
    pub top_dedup_pairs: Vec<DedupPairStat>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedupPairStat {
    pub table_a: String,
    pub table_b: String,
    pub shared_partitions: usize,
    pub shared_rows: u64,
}

pub struct DedupAnalyzer;

impl DedupAnalyzer {
    pub fn compute_stats(per_table: &[(String, Vec<PartitionInfo>)]) -> DedupStats {
        let mut index: HashMap<(u64, u64), Vec<String>> = HashMap::new();
        let mut total_partitions = 0;
        let mut total_rows = 0;

        for (table, parts) in per_table {
            for p in parts {
                total_partitions += 1;
                total_rows += p.row_count;
                index
                    .entry((p.checksum, p.row_count))
                    .or_default()
                    .push(table.clone());
            }
        }

        let unique_partitions = index.len();
        let duplicate_partitions = total_partitions - unique_partitions;

        let mut duplicate_rows = 0;
        let mut pair_counts: HashMap<(String, String), (usize, u64)> = HashMap::new();

        for ((_checksum, row_count), holders) in &index {
            if holders.len() > 1 {
                let extra = holders.len() as u64 - 1;
                duplicate_rows += extra * row_count;

                let mut tables: Vec<String> = holders.clone();
                tables.sort();
                tables.dedup();

                for i in 0..tables.len() {
                    for j in (i + 1)..tables.len() {
                        let a = tables[i].clone();
                        let b = tables[j].clone();
                        let entry = pair_counts.entry((a, b)).or_insert((0, 0));
                        entry.0 += 1;
                        entry.1 += row_count;
                    }
                }
            }
        }

        let estimated_savings_pct = if total_rows == 0 {
            0.0
        } else {
            (duplicate_rows as f64 / total_rows as f64) * 100.0
        };

        let mut top_dedup_pairs: Vec<DedupPairStat> = pair_counts
            .into_iter()
            .map(|((a, b), (count, rows))| DedupPairStat {
                table_a: a,
                table_b: b,
                shared_partitions: count,
                shared_rows: rows,
            })
            .collect();

        top_dedup_pairs.sort_by(|a, b| b.shared_partitions.cmp(&a.shared_partitions));

        DedupStats {
            total_partitions,
            unique_partitions,
            duplicate_partitions,
            total_rows,
            estimated_savings_pct,
            top_dedup_pairs: top_dedup_pairs.into_iter().take(10).collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    pub key: String,
    pub checksum: u64,
    pub row_count: u64,
}
