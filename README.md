# DataForge

**The High-Performance, Analyst-Friendly Data Transformation Engine.**

DataForge is a modern data modeling framework built in Rust, designed for speed, reproducibility, and flexibility. It combines the power of Starlark (Python dialect) for configuration and shared logic with the performance of warehouse-native SQL for data processing.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/rust-stable-brightgreen.svg)](https://www.rust-lang.org/)

## 🚀 Key Features

- **Hybrid Modeling**: Define model metadata in Starlark headers and logic in standard SQL.
- **Global Macros**: Share complex logic across your project using `.stark` macro files.
- **Environment-Aware**: Seamlessly plan and deploy transformations across `dev`, `staging`, and `prod`.
- **Intelligent Planning**: Automated topological sorting and content-based hashing (`SHA-256`) for incremental updates.
- **Multi-Dialect Support**: Native support for Snowflake, BigQuery, Postgres, SQLite, and more.
- **Fast Watcher**: Real-time background model registration for rapid development loops.

## 📦 Installation

```bash
# Clone the repository
git clone https://github.com/randodev95/dataforge
cd dataforge

# Build the project
cargo build --release
```

## 🛠️ Quick Start

1. **Initialize a new project**:
   ```bash
   dataforge init my_project
   ```

2. **Add a model** (`models/bronze/raw_data.sql`):
   ```sql
   ---
   model(name='raw_data')
   ---
   SELECT * FROM read_parquet('data.parquet')
   ```

3. **Plan the transformation**:
   ```bash
   dataforge plan --project my_project --dialect duckdb
   ```

## 📖 Documentation

For detailed guides, see:
- [How to Use DataForge](HOW_TO_USE.md)
- [Architecture Overview](docs/ARCHITECTURE.md)

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for more details.

## 📄 License

This project is licensed under the MIT License.
