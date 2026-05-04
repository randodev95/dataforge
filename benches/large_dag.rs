use criterion::{criterion_group, criterion_main, Criterion};
use titan_engine::{Filler, StateStore, Muscle, VDE};
use titan_engine::filler::dag::ModelTask;
use titan_engine::materialize::Materialization;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::runtime::Runtime;
use tempfile::tempdir;

fn large_dag_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let dir = tempdir().unwrap();
    let state_store = StateStore::open(dir.path()).unwrap();
    let filler = Filler::new(state_store, dir.path(), 4);
    
    let muscle = Arc::new(Muscle::new());
    let vde = Arc::new(VDE::new(muscle.clone()));

    let mut tasks = Vec::new();
    for i in 0..1000 {
        tasks.push(ModelTask {
            name: format!("model_{}", i),
            env: "bench".to_string(),
            raw_sql: "SELECT 1".to_string(),
            config: HashMap::new(),
            fingerprinter: filler.fingerprinter.clone(),
            state_store: filler.state_store.clone(),
            muscle: muscle.clone(),
            vde: vde.clone(),
            parent_names: if i > 0 { vec![format!("model_{}", i-1)] } else { vec![] },
            materialization: Materialization::View,
            unique_key: None,
            target_type: "local".to_string(),
            retention: None,
            on_schema_change: titan_engine::project::OnSchemaChange::AppendOnly,
            plan_only: true, // benchmark orchestration, not physical execution
            semaphore: filler.semaphore.clone(),
        });
    }

    c.bench_function("run_1000_model_dag_plan", |b| {
        b.to_async(&rt).iter(|| async {
            filler.run_dag(tasks.clone()).await.unwrap();
        });
    });
}

criterion_group!(benches, large_dag_benchmark);
criterion_main!(benches);
