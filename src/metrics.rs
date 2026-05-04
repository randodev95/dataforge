use prometheus::{Counter, Registry, opts, register_counter_vec, register_histogram_vec, CounterVec, HistogramVec};
use std::sync::LazyLock;

pub static REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::new);

pub static MATERIALIZATIONS_TOTAL: LazyLock<CounterVec> = LazyLock::new(|| {
    register_counter_vec!(
        opts!(
            "titan_materializations_total",
            "Total number of materializations performed"
        ),
        &["type", "status"]
    ).expect("Can't register MATERIALIZATIONS_TOTAL")
});

pub static MATERIALIZATION_LATENCY_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "titan_materialization_latency_seconds",
        "Time taken to materialize models",
        &["type"]
    ).expect("Can't register MATERIALIZATION_LATENCY_SECONDS")
});

pub static ROWS_WRITTEN_TOTAL: LazyLock<CounterVec> = LazyLock::new(|| {
    register_counter_vec!(
        opts!(
            "titan_rows_written_total",
            "Total number of rows written to storage"
        ),
        &["type"]
    ).expect("Can't register ROWS_WRITTEN_TOTAL")
});

pub static STATE_STORE_ERRORS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    prometheus::register_counter!(
        opts!(
            "titan_state_store_errors_total",
            "Total number of errors encountered by the state store"
        )
    ).expect("Can't register STATE_STORE_ERRORS_TOTAL")
});

pub fn register_metrics() {
    // Accessing Lazy statics to trigger registration
    let _ = *MATERIALIZATIONS_TOTAL;
    let _ = *MATERIALIZATION_LATENCY_SECONDS;
    let _ = *ROWS_WRITTEN_TOTAL;
    let _ = *STATE_STORE_ERRORS_TOTAL;
}
