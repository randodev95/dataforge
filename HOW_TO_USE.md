# How to Use DataForge

This guide provides a hands-on introduction to setting up and running your first DataForge project.

## 1. Project Initialization

Start by creating a structured project directory. DataForge uses a specific folder layout to automate model discovery.

```bash
dataforge init my_project
```

This creates the following structure:
- `models/`: Where your SQL transformations live.
  - `bronze/`: Raw data ingestion.
  - `silver/`: Cleaning and normalization.
  - `gold/`: Analytics-ready tables.
- `macros/`: Shared Starlark logic (`.stark` files).
- `dataforge.yaml`: Project configuration.

## 2. Defining Your First Model

Create a file at `models/bronze/users.sql`. DataForge models use a **hybrid format**: a Starlark metadata header followed by a SQL body.

```sql
---
model(
    name = "raw_users",
    columns = ["id", "email", "created_at"]
)
---
SELECT * FROM read_csv('users.csv')
```

### Key Metadata Fields:
- `name`: Unique identifier for the model.
- `columns`: (Optional) Expected column list for contract validation.
- `watermark`: (Optional) Column name used for incremental loading.

## 3. Using Macros

Shared logic lives in `macros/*.stark`. These are global and can be referenced in any model header.

**Create `macros/utils.stark`**:
```python
def clean_name(name):
    return name.strip().lower()
```

**Use it in a model**:
```sql
---
model(name = clean_name("  Analytics_Users  "))
---
SELECT * FROM {{ref('raw_users')}}
```

## 4. Running a Plan

The `plan` command calculates the dependency graph (DAG) and identifies what needs to be updated based on content changes.

```bash
dataforge plan --project . --from dev --to prod --dialect postgres
```

### Understanding the Output:
- **Update**: The model or its dependencies have changed. DataForge provides the new SQL and a unique content hash.
- **Remove**: The model file was deleted from the source environment and should be pruned from the target.

## 5. Development Workflow

For the best experience, keep a terminal open with the DataForge watcher or run plans frequently as you edit. Because DataForge uses **Topological Sorting**, it will always ensure parent models are planned before their children.
