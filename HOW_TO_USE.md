# How to Use Titan Engine

This guide provides step-by-step recipes for production-grade data materialization with Titan. It covers everything from initial setup to high-scale operational control.

## Prerequisites

- [Rust toolchain](https://rustup.rs/) (v1.80+)
- [Git](https://git-scm.com/)
- Environment variables for secrets resolution (optional)

## Installation

```bash
git clone https://github.com/randodev95/dataforge.git
cd dataforge
cargo install --path .
```

## Initializing a Project

Create the standard structure for a data warehouse project:

```bash
titan init my_warehouse
cd my_warehouse
```

## Recipe: Secure Secrets Management

Titan supports environment variable interpolation for sensitive connection strings.

1. Define a secret in your environment:
   ```bash
   export DB_PASSWORD=supersecret
   ```
2. Reference it in `profiles.yml` using `${VAR}` syntax:
   ```yaml
   prod:
     target_type: delta
     connection_string: "postgres://user:${DB_PASSWORD}@localhost/db"
   ```

Titan will resolve the secret at runtime and automatically mask it in all logs (`password=******`).

## Recipe: Operational Control & Scale

### Validating in CI/CD
Use the `check` command to validate project structure and SQL syntax without executing data:

```bash
titan check --target prod
```

### High-Concurrency Execution
For large DAGs, control the number of parallel tasks using the `--jobs` flag:

```bash
titan run --target prod --jobs 8
```

### Real-time Observability
Enable the built-in Prometheus metrics server to monitor materialization latency and row counts:

```bash
titan run --target prod --metrics
```
Metrics are exposed at `http://localhost:9090`.

### Mission Control Dashboard
Titan includes a high-fidelity web dashboard for real-time monitoring and lineage exploration.

1. **Serve the UI**:
   ```bash
   cd ui && python3 -m http.server 8000
   ```
2. **Access Mission Control**: Navigate to `http://localhost:8000` to view:
   - **Performance Charts**: Real-time throughput velocity.
   - **Column Lineage**: Interactive heritage tracing for every model.
   - **Audit Logs**: Deep history explorer for the `titan_audit` Delta stream.

## Recipe: Advanced Materialization

### SCD-2 Snapshots
To track historical changes (Slowly Changing Dimensions Type 2), use the `snapshot` strategy in your model config:

```sql
{{ config(
    materialization='snapshot',
    unique_key='user_id',
    retention_days=30
) }}
SELECT * FROM users
```
Titan will manage `titan_valid_from`, `titan_valid_to`, and `titan_logic_hash` columns automatically.

### Incremental Merges
For high-volume tables, use `incremental` materialization to perform atomic upserts based on a unique key:

```sql
{{ config(
    materialization='incremental',
    unique_key='event_id',
    on_schema_change='append'
) }}
SELECT * FROM raw_events
```

## Reliability Features

- **RAII Table Isolation**: Titan uses UUID-based namespacing for temporary tables, ensuring parallel runs never collide.
- **Atomic State**: All metadata updates are committed to the state store using atomic write batches.
- **Graceful Shutdown**: Titan handles `SIGINT` (Ctrl+C) gracefully, cancelling the DAG and cleaning up temporary resources before exiting.

---

For technical API details or core architecture, please refer to the `TITAN_SPEC.md`.
