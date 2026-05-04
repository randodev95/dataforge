# Titan Engine

[![License: Elastic-2.0](https://img.shields.io/badge/License-Elastic--2.0-blue.svg)](LICENSE)
[![Rust](https://github.com/randodev95/dataforge/actions/workflows/rust.yml/badge.svg)](https://github.com/randodev95/dataforge/actions)

**Titan Engine** is a high-performance, vectorized SQL execution engine designed for reliable data materialization at scale. Built in Rust and powered by Apache DataFusion and Delta Lake, Titan implements a "Serialized Pipe" architecture to bypass the complexities of AST sharing across polyglot SQL environments.

## 🚀 Key Features

- **Vectorized Execution**: Native support for high-throughput data processing via DataFusion and Arrow.
- **Atomic SCD-2 Snapshots**: Built-in support for Slowly Changing Dimensions (Type 2) with historical window tracking.
- **Secure Secrets Management**: Environment variable interpolation (`${VAR}`) and automatic credential masking in logs.
- **Real-time Observability**: Embedded Prometheus metrics server for tracking materialization latency and row counts.
- **Intelligent Orchestration**: Parallel DAG execution with RAII table isolation and logic-hash change tracking.
- **Deployability**: Built-in `titan check` command for CI/CD validation without execution.

## 🛡️ Why Titan?

Titan is built for production reliability. Unlike traditional tools, Titan enforces:
- **Atomicity**: Metadata and indices are updated via atomic write batches in RocksDB.
- **Isolation**: Each execution uses unique UUID namespaces to prevent parallel run collisions.
- **Portability**: Serialized SQL approach ensures logic remains 99.9% portable across Postgres, BigQuery, and Snowflake.

## 📦 Installation

```bash
git clone https://github.com/randodev95/dataforge.git
cd dataforge
cargo build --release
```

## 🛠 Usage

1. **Initialize a Project**:
   ```bash
   titan init my_project
   ```

2. **Validate Project**:
   ```bash
   titan check --target prod
   ```

3. **Run with Metrics**:
   ```bash
   titan run --target prod --metrics --jobs 4
   ```

For detailed instructions, see the [How-To Guide](HOW_TO_USE.md).

## 📄 License

This project is licensed under the **Elastic License v2**. See the [LICENSE](LICENSE) file for details.

---
Built with 🦀 by the DataForge Team.
