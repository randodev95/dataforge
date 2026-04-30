# DataForge 2.0

**The High-Performance, Reactive Data Orchestration Engine.**

DataForge is a Rust-native transformation engine designed to bridge the gap between static logic validation and stateful warehouse execution. It implements the **"Pipeline of Truth"**—integrating **SDF** for logic checks and **Rocky** for zero-copy state management.

[![License: ELv2](https://img.shields.io/badge/License-ELv2-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-stable-brightgreen.svg)](https://www.rust-lang.org/)

## 🚀 Key Features

- **Pipeline of Truth**: Interweaves SDF (Static Logic Analysis) and Rocky (Warehouse State Management) into a single reactive flow.
- **Microsecond Macros**: Native Starlark expansion. Replaces Jinja with sub-millisecond `${ref()}` resolution.
- **Reactive Engine**: `serve` mode with background file watching and JSON-RPC API for real-time frontend feedback.
- **WASM Plugin System**: Sandboxed, language-agnostic extensibility for governance and custom data checks.
- **Zero-Copy Readiness**: Designed for modern data warehouses using Rocky's virtual environment planning.

## ⚡ Performance

| Operation | dbt (Python) | DataForge (Rust) | Factor |
|-----------|--------------|------------------|--------|
| Startup | ~1s | **~5ms** | 200x |
| Macro Expansion | ~1-3s | **~150μs** | 1000x+ |

## 🛠️ Quick Start

1. **Install and Init**:
   ```bash
   cargo build --release
   ./target/release/dataforge init my_project
   ```

2. **Start the Reactive Engine**:
   ```bash
   dataforge serve --project . --port 8080
   ```

3. **Define Your Logic**:
   Edit models in `models/*.sql`. Use `${ref('other_model')}` for dependencies. Watch the `serve` logs for instant validation.

## 📖 Documentation

- [How to Use DataForge](HOW_TO_USE.md): Practical guides for building pipelines.
- [Architecture](docs/ARCHITECTURE.md): Deep dive into the Orchestrator and Bridges.

## 📄 License

Licensed under the **Elastic License v2**. See [LICENSE](LICENSE) for details.
