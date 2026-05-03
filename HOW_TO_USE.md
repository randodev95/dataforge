# How to Use Titan Engine

This guide provides step-by-step recipes for the most common tasks in Titan Engine, from initializing a new project to running a complex data materialization pipeline.

## Prerequisites

Before you begin, ensure you have the following installed:
- [Rust toolchain](https://rustup.rs/) (v1.80+)
- [Git](https://git-scm.com/)

## Installation

Build the Titan CLI from source:

```bash
git clone https://github.com/randodev95/dataforge.git
cd dataforge
cargo install --path .
```

This will add `titan-engine` (aliased as `titan`) to your PATH.

## Initializing a New Project

To create a new data engineering project structure:

```bash
titan init my_data_warehouse
cd my_data_warehouse
```

This creates a standard project layout:
- `models/`: Your SQL transformations.
- `seeds/`: Small static datasets (CSV).
- `exposures/`: Downstream usage definitions.
- `profiles.yml`: Environment configurations (dev/prod).

## Adding a New Model

Create a `.sql` file in the `models/` directory. You can use Jinja2-style references to link models together:

```sql
-- models/active_users.sql
SELECT 
    id, 
    name 
FROM {{ ref('raw_users_seed') }} 
WHERE status = 'active'
```

## Running the Pipeline

### 1. Planning Changes
Use the `plan` command to see what Titan *would* do without executing any DDL:

```bash
titan plan --target dev
```

### 2. Executing Transformations
Run the pipeline to materialize your models in the target environment:

```bash
titan run --target prod
```

Titan will:
- Parse and render your templates.
- Calculate **Logic Hashes** to determine which models have changed.
- Orchestrate parallel execution of required models.
- Perform an **Atomic Pointer Swap** to deploy the new data.

## Verifying Data Quality

Titan supports dbt-style assertions to ensure your data remains clean:

```bash
titan test --target prod
```

Common tests include `unique` and `not_null` checks on materialized views.

## Inspecting Project Exposures

To view a summary of how your data is being consumed:

```bash
titan exposure list
```

---

For technical API details or core architecture, please refer to the `TITAN_SPEC.md`.
