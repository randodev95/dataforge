# Titan Engine

[![License: Elastic-2.0](https://img.shields.io/badge/License-Elastic--2.0-blue.svg)](LICENSE)
[![Rust](https://github.com/randodev95/dataforge/actions/workflows/rust.yml/badge.svg)](https://github.com/randodev95/dataforge/actions)

**Titan Engine** is a high-performance, vectorized SQL execution engine designed for reliable data materialization at scale. Built in Rust and powered by Apache DataFusion, Titan implements a "Serialized Pipe" architecture to bypass the complexities of AST sharing across polyglot SQL environments.

## 🚀 Key Features

- **Vectorized Execution**: Native support for high-throughput data processing via DataFusion and Arrow.
- **Three 9s Portability**: Uses a serialized SQL approach to ensure logic remains 99.9% portable across different warehouses (Postgres, BigQuery, Snowflake).
- **Atomic Pointer Swaps**: Implements a Virtual Deployment Environment (VDE) for blue-green materialization, ensuring zero-downtime deployments of data models.
- **Intelligent Orchestration**: Parallel DAG execution with smart-skip logic and logic-hash change tracking.
- **Stateful Reliability**: Powered by a high-performance RocksDB state store for tracking materialization metadata and fingerprints.

## 📦 Installation

To build Titan Engine from source, you need the [Rust toolchain](https://rustup.rs/) installed.

```bash
git clone https://github.com/randodev95/dataforge.git
cd dataforge
cargo build --release
```

The binary will be available at `target/release/titan-engine`.

## 🛠 Usage

1. **Initialize a Project**:
   ```bash
   titan init my_project
   cd my_project
   ```

2. **Run the Pipeline**:
   ```bash
   titan run --target prod
   ```

3. **Verify Data Quality**:
   ```bash
   titan test --target prod
   ```

For detailed instructions, see the [How-To Guide](HOW_TO_USE.md).

## 📄 License

This project is licensed under the **Elastic License v2**. See the [LICENSE](LICENSE) file for details.

---
Built with 🦀 by the DataForge Team.
