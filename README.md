# Nexus Runtime v1.0

**Causally-consistent, crash-recoverable, deterministic execution substrate for autonomous agent systems.**

Nexus is not an agent framework. It is infrastructure that makes agent execution durable, auditable, and portable.

## Quick Start

```bash
# Build
cargo build --release

# Run tests (including Phoenix recovery tests)
cargo test

# Phoenix invariant tests
cargo test --package phoenix-tests

# Run demo
./scripts/demo.sh

# CLI usage
nexus run "refactor authentication to JWT" --budget 5.00
nexus status <session-id>
nexus resume <session-id>
```

## Architecture

```
L5: Agent Interface Adapters (OpenClaw, Hermes, Cursor, Claude Code)
L4: Nexus Kernel (State Machine, Event Store, Scheduler, Entropy, Cost Governor)
L3: Worker Fabric (Python, Node.js, Rust, WASM — JSON-RPC over stdio)
L2: Causal Memory & Persistence (Event Log, Memory Graph, Content Vault)
L1: External Toolchain (MCP, LLM APIs, GitHub/Email)
```

## Core Principles

1. **Event Log is Source of Truth** — All state mutations append to immutable log
2. **State is Materialized View** — Deletable, rebuildable from events
3. **transition() is Pure Function** — No IO, no clock, no random
4. **Workers are Stateless** — No network, no persistent state
5. **Crash Recovery (Phoenix)** — Survive kill -9 without LLM re-calls

## Deployment Modes

| Mode | Storage | Scheduler | Infrastructure |
|------|---------|-----------|----------------|
| Lite | SQLite (WAL) | Local process | Zero |
| Pro | PostgreSQL | Docker | Docker |
| Enterprise | PostgreSQL + Temporal | Kubernetes | K8s + Temporal |

## Crates

| Crate | Description |
|-------|-------------|
| `nexus-core` | State machine, types, event system, recovery, effects |
| `nexus-event-store` | SQLite/PostgreSQL event store implementations |
| `nexus-rpc` | JSON-RPC 2.0 codec for Worker communication |
| `nexus-security` | Capability tokens (HMAC-SHA256), sandboxing |
| `nexus-scheduler` | Topological + capability-aware task scheduling |
| `phoenix-tests` | 8-invariant crash recovery test framework |

## Phoenix Recovery Guarantees

The system survives `kill -9` at any execution step and resumes without:
- Re-calling LLMs (cached results)
- Duplicating side effects (2-phase intent protocol)
- Losing causal ordering (vector clocks)
- State corruption (event-sourced replay)

## License

MIT OR Apache-2.0 (dual-licensed)
