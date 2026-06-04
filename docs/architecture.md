# Nexus Runtime Architecture

See the [Technical Specification](../td.md) for the complete architecture document.

## Layer Model

```
L5: Agent Interface Adapters (OpenClaw, Hermes, CLI)
L4: Nexus Kernel (State Machine, Event Store, Scheduler)
L3: Worker Fabric (Python, Node.js, Rust, WASM)
L2: Causal Memory & Persistence
L1: External Toolchain (MCP, LLM APIs)
```

## Deployment Modes

- **Lite:** SQLite + local process — zero infrastructure
- **Pro:** PostgreSQL + Docker — team collaboration
- **Enterprise:** PostgreSQL + Temporal + K8s — production multi-agent

## Key Files

| File | Description |
|------|-------------|
| `Cargo.toml` | Workspace with 11 crates |
| `crates/nexus-core/` | State machine, types, recovery |
| `crates/nexus-event-store/` | SQLite/PostgreSQL event store |
| `crates/phoenix-tests/` | 8-invariant crash recovery tests |
| `deny.toml` | cargo-deny license + advisory config |
| `.clippy.toml` | Disallowed types for determinism |
