# Nexus Runtime

Causally-consistent execution substrate for autonomous agent systems.

**Event log is the source of truth. State is a materialized view. Workers are stateless. The Kernel owns causality.**

## Quick Start

```bash
# Build
cargo build --bin nexus

# Run a session
./target/debug/nexus run "your intent here"

# Check status
./target/debug/nexus status <session-id>

# View event log
./target/debug/nexus log <session-id>

# Simulate crash recovery
./target/debug/nexus resume <session-id>

# Run all tests
cargo test
```

## Architecture

```
L5: Agent Interface Adapters (OpenClaw / Hermes / CLI)
L4: Nexus Kernel — Causal State Machine, Event Store, Recovery, Worker Scheduler
L3: Worker Fabric — Python / Node.js / Rust / WASM (JSON-RPC 2.0 over stdio)
L2: Causal Memory & Persistence — Event Log, Memory Graph, Vector Index, Content Vault
L1: External Toolchain — MCP Servers, LLM APIs, GitHub/Email/Browser
```

## Core Principles

- `transition()` is a pure function — no async, no I/O, no clock, no random
- Event log is append-only, immutable
- Deterministic serialization (BTreeMap, u64, rmp-serde)
- Workers are stateless — no persistent memory, no network, no direct LLM
- All side effects go through two-phase intent protocol

## Deployment Modes

| Mode | Storage | Scheduler | Use Case |
|------|---------|-----------|----------|
| Lite | SQLite (WAL) | Local process | Personal CLI tools |
| Pro | PostgreSQL | Docker containers | Team collaboration |
| Enterprise | PostgreSQL + Temporal | Kubernetes | Production multi-agent |

## License

MIT OR Apache-2.0
