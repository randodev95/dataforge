# How-to Guide: Establish a Reactive Data Pipeline

This guide shows you how to use DataForge 2.0 to create a "Pipeline of Truth"—a high-performance, reactive development environment that validates logic via SDF and plans execution via Rocky.

## Prerequisites
- Rust (stable)
- SDF CLI installed in PATH
- DuckDB (optional, for previews)

## 1. Initialize Your Project
Create a new project structure compatible with the 2.0 Orchestrator.

```bash
dataforge init my_project
cd my_project
```

This creates:
- `models/`: Your SQL logic.
- `macros/`: Shared Starlark functions.
- `plugins/`: WASM governance plugins.

## 2. Launch the Reactive Engine
Instead of manual planning, start the DataForge 2.0 **Serve** mode. This monitors your files and provides real-time feedback.

```bash
dataforge serve --project . --port 8080
```

The engine now watches for changes and exposes a JSON-RPC API for IDE integration.

## 3. Define a Model with Macros
Use the new `${ref()}` syntax for microsecond-fast macro expansion.

**models/stg_orders.sql**:
```sql
SELECT 
    order_id,
    customer_id,
    amount
FROM raw_orders
```

**models/fct_orders.sql**:
```sql
SELECT 
    o.order_id,
    c.name as customer_name,
    o.amount
FROM ${ref('stg_orders')} o
JOIN ${ref('stg_customers')} c ON o.customer_id = c.customer_id
```

## 4. Validate Logic (SDF Bridge)
When you save a file, DataForge automatically triggers the **SDF Bridge**. 
- It catchs column-name typos and type mismatches instantly.
- Look at the `serve` logs for logic errors before they hit the warehouse.

## 5. Plan State (Rocky Bridge)
Once logic is green, DataForge uses the **Rocky Bridge** to calculate zero-copy clones or deployment plans. This ensures your development environment matches production without data duplication.

## 6. Add Custom WASM Validation
Create a sandboxed check for data quality or governance.

1. Write a Rust plugin in `plugins/check_amount/`.
2. Compile to WASM.
3. DataForge will execute this plugin as part of the orchestration pipeline.

---
**Next Step**: Learn more about the [Architecture](docs/ARCHITECTURE.md).
