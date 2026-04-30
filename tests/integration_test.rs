use DataForge::{Engine, types::{EnvName}, Action, Scheduler, db::DuckDBConnector};

#[tokio::test]
async fn test_nyc_taxi_dag_full() {
    let dev = EnvName("dev".to_string());
    let prod = EnvName("prod".to_string());
    
    let mut engine = Engine::new();

    // Bronze: Raw Parquet
    engine.register_model(&dev, "model(name='bronze', query=\"SELECT fare_amount, trip_distance FROM read_parquet('taxi_data.parquet')\")").unwrap();
    
    // Silver: Filtered - Use ref() to track dependency
    engine.register_model(&dev, "model(name='silver', query='SELECT fare_amount, trip_distance FROM ' + ref('bronze') + ' WHERE fare_amount > 0', columns=['fare_amount'])").unwrap();

    let plan = engine.plan(&dev, &prod).unwrap();
    assert_eq!(plan.actions.len(), 2);
    
    if let Action::Update(m, ..) = &plan.actions[0] {
        assert_eq!(m.name.0, "bronze");
    }

    let conn = duckdb::Connection::open_in_memory().unwrap();
    let connector = DuckDBConnector::new(conn);
    let scheduler = Scheduler;

    scheduler.run_plan(&engine, plan, &connector).await.expect("Physical execution failed");
}
