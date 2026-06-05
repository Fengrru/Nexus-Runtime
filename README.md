# Nexus Runtime

**Causally-consistent execution substrate for autonomous agent systems.**

[![Rust](https://img.shields.io/badge/rust-1.80+-orange.svg)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)
[![Tests](https://img.shields.io/badge/tests-132%20passed-brightgreen.svg)]()

Nexus Runtime is not an agent framework, not a chatbot wrapper, not a cloud SaaS. It is infrastructure that makes agent execution durable, auditable, and portable.

**Core principle:** The event log is the source of truth. State is a materialized view. Workers are stateless. The Kernel owns causality.

---

## What It Does

Nexus takes a user intent (plain English), plans via LLM, dispatches workers via JSON-RPC over stdio, checkpoints every action to an append-only event log, and can recover from any crash at any point — without duplicating LLM calls or side effects.

```
User intent
    ↓
LLM generates execution plan (OpenAI / Anthropic / DeepSeek)
    ↓
State machine transitions: Created → Intake → Planning → Planned → Executing
    ↓
Worker spawned via JSON-RPC 2.0 over stdio (Python / Node.js / Rust / WASM)
    ↓
Each action checkpoints to append-only event log
    ↓
Crash at any point → recover from last checkpoint, never re-call LLM
```

### Real-world Demo

```bash
$ export DEEPSEEK_API_KEY="sk-..."

$ nexus run "Read auth.js and user.js, audit security flaws, write report to audit-report.md" \
    --model deepseek-chat

[LLM]    deepseek-chat → 151 in, 777 out, $0.01, 6923ms
         Plan: [read_file(auth.js), read_file(user.js), analyze, write_report]

[WORKER] Spawned PID 12964
         ✅ Step 1/4: read_file  auth.js          [CKPT]
         ✅ Step 2/4: read_file  user.js          [CKPT]
         ✅ Step 3/4: run_command analysis        [CKPT]
         ✅ Step 4/4: write_file audit-report.md  [CKPT]
[OK]     Worker completed → Executing

$ nexus resume <session-id>
[OK] Session recovered  Status: Executing  Causal check: true  Replay check: true
```

---

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│ L5: Agent Interface Adapters                             │
│    OpenClaw Gateway / Hermes CLI / Nexus CLI             │
├──────────────────────────────────────────────────────────┤
│ L4: Nexus Kernel (Rust)                                  │
│    Causal State Machine · Event Store · Recovery Manager │
│    Worker Scheduler · Entropy Controller · Side-Effect   │
│    Guard · Cost Governor · LLM Proxy                     │
├──────────────────────────────────────────────────────────┤
│ L3: Worker Fabric                                        │
│    Python · Node.js · Rust · WASM (JSON-RPC 2.0 / stdio) │
│    No ports, no network access, no persistent state      │
├──────────────────────────────────────────────────────────┤
│ L2: Causal Memory & Persistence                          │
│    Event Log · Memory Graph · Content Vault (BLAKE3)     │
│    Vector Clock · Two-Phase Commit                       │
├──────────────────────────────────────────────────────────┤
│ L1: External Toolchain                                   │
│    LLM APIs (OpenAI / Anthropic / DeepSeek)              │
│    Docker / Kubernetes · OPA/Rego Policies               │
└──────────────────────────────────────────────────────────┘
```

---

## Crates

| Crate | Purpose |
|-------|---------|
| `nexus-core` | Types, state machine (`transition()` pure function), events, checkpoints, memory graph, recovery, side-effect guard, entropy controller, protocol, LLM proxy, vault, WASM sandbox, worker spawner |
| `nexus-event-store` | EventStore trait + SQLite, PostgreSQL implementations with full schema (9 tables, foreign keys, WAL) |
| `nexus-rpc` | JSON-RPC 2.0 codec over stdio (NDJSON framing) |
| `nexus-security` | HMAC-SHA256 capability tokens, sandbox tiers (Landlock/seccomp/audit), path traversal protection |
| `nexus-scheduler` | Local, Docker (Bollard), and Kubernetes (kube-rs) worker schedulers with capability-aware dispatch |
| `nexus-cli` | CLI binary (`nexus run`, `status`, `log`, `resume`, `suspend`, `archive`, `export`, `import`) |
| `nexus-metrics` | Prometheus metrics for events, transitions, workers, LLM calls, entropy |
| `nexus-coordinator` | Multi-agent coordination |
| `nexus-message-bus` | Distributed causal bus |
| `nexus-temporal` | Temporal durable execution adapter |
| `phoenix-tests` | Acceptance test framework — 8 invariants, 6 kill-9 phase tests, 10 Phoenix suite tests, 4 cross-tool migration tests |
| `openclaw-adapter` | OpenClaw Gateway session bridging with HTTP integration |
| `hermes-adapter` | Hermes CLI checkpoint persistence and file-based session transfer |
| `nexus-sdk` | Rust SDK re-exporting nexus-core |
| `rust-worker` | Rust worker implementing JSON-RPC 2.0 over stdio |

---

## Design Invariants

| Constraint | Enforcement |
|------------|------------|
| `transition()` is a pure function | No async, no I/O, no clock, no random — verified by `cargo test` |
| Event log is append-only | No UPDATE/DELETE on events table — enforced at schema level |
| Deterministic serialization | `BTreeMap` for maps, `u64` for timestamps/currency, `rmp-serde` (MessagePack) |
| Workers are stateless | No persistent memory, no network, no direct LLM API access |
| LLM outputs are externalized events | Results cached; never re-called during recovery |
| Side effects are two-phase | Intent → Validate → Execute → Commit. Classification: Pure/Idempotent/Reversible/Irreversible |
| Phoenix gate | All 8 invariants must pass before release |

---

## Supported LLM Providers

| Provider | API Base | Model Examples | Env Variable |
|----------|----------|---------------|--------------|
| OpenAI | `api.openai.com` | `gpt-4o`, `gpt-4o-mini` | `OPENAI_API_KEY` |
| Anthropic | `api.anthropic.com` | `claude-3.5-sonnet` | `ANTHROPIC_API_KEY` |
| DeepSeek | `api.deepseek.com` | `deepseek-chat`, `deepseek-reasoner` | `DEEPSEEK_API_KEY` |

Without an API key, the LLM proxy falls back to simulation mode.

---

## Deployment Modes

| Mode | Storage | Scheduler | Requirements |
|------|---------|-----------|--------------|
| **Lite** | SQLite (WAL) | Local process | Zero dependencies |
| **Pro** | PostgreSQL | Docker | Docker daemon |
| **Enterprise** | PostgreSQL + Temporal | Kubernetes | K8s cluster |

All three modes share identical protocol semantics and state machine behavior.

---

## Quick Start

### Prerequisites

- Rust 1.80+ (`rustup`)
- Optional: Python 3.11+ (for Python worker), Node.js 20+ (for Node.js worker)

### Build & Test

```bash
# Clone
git clone https://github.com/nexus-runtime/nexus.git
cd nexus

# Build
cargo build --bin nexus

# Run tests (132 tests, all must pass)
cargo test

# Run Phoenix acceptance tests
cargo test --package phoenix-tests

# Check code quality
cargo clippy --all-targets
```

### Run a Session

```bash
# Lite mode — zero infrastructure, uses SQLite
./target/debug/nexus run "read the README and summarize it"

# With a real LLM
export DEEPSEEK_API_KEY="sk-..."
./target/debug/nexus run "analyze auth.js for security vulnerabilities" --model deepseek-chat
```

### Inspect & Recover

```bash
# Check session status
./target/debug/nexus status <session-id>

# View event log (immutable, append-only)
./target/debug/nexus log <session-id> --limit 20

# Simulate crash recovery
./target/debug/nexus resume <session-id>

# Export for cross-tool migration
./target/debug/nexus export <session-id> --output session.nexus

# Import from another tool
./target/debug/nexus import session.nexus
```

### Docker Deploy

```bash
# Lite mode
docker-compose -f docker-compose.lite.yml up

# Pro mode (requires PostgreSQL + Redis)
docker-compose -f docker-compose.pro.yml up
```

---

## State Machine

```
CREATED → INTAKE → PLANNING → PLANNED → EXECUTING → CHECKPOINTING → EXECUTING → ...
                                        ↘ CONVERGING → REFLECTING → COMPLETED
                                        ↘ FAILED

Any state → HumanApprovalRequested → BLOCKED → HumanApproved → EXECUTING
Any state → SessionSuspended → CHECKPOINTING → SessionResumed → EXECUTING
Any state → SessionArchived
```

12 session states, 25+ event types, all transitions enforced by the `transition()` pure function.

---

## Worker Protocol

Workers communicate via **JSON-RPC 2.0 over stdio** with NDJSON framing:

```
Kernel → Worker:  execute { task_id, intent, capabilities, timeout }
Worker → Kernel:  checkpoint { step_index, actions, artifacts }
Worker → Kernel:  result { status, artifacts, metrics }
Kernel → Worker:  cancel { task_id, reason }
```

Workers have no network access, no persistent state, and receive capability tokens for every action.

---

## Causal Consistency

Every event carries a vector clock (`BTreeMap<SessionId, u64>`). The state machine enforces monotonicity — events must have a causal vector ≥ the current state. This guarantees:

- **Happens-before** ordering across distributed sessions
- **Detection of concurrent** conflicting events
- **Deterministic replay** — same events → same state

---

## Testing

```bash
cargo test                           # 132 tests, all passing
cargo test --package phoenix-tests   # 26 Phoenix tests
cargo bench --bench benchmarks       # Performance benchmarks
cargo clippy --all-targets           # Zero warnings
cargo deny check                     # License/security audit
```

### Phoenix Invariants

| I-# | Invariant |
|-----|-----------|
| I-1 | State Authority — database passes integrity check |
| I-2 | Checkpoint Identity — checkpoint ID and step survive restart |
| I-3 | Replay Integrity — event replay produces byte-identical state |
| I-4 | Artifact Integrity — BLAKE3 hashes of vault files remain valid |
| I-5 | Determinism Context — seed, model, input_hash preserved |
| I-6 | Cost Integrity — no duplicated LLM calls |
| I-7 | Resume Continuity — execution resumes from step N+1 |
| I-8 | Eventual Consistency — all transitions reconstructable from event log |

### Kill-9 Tests

Recovery is tested at every phase: intake, planning, executing, checkpoint, converging, reflecting — plus worker crash, LLM timeout, side-effect crash, and cross-session resume.

---

## Project Structure

```
nexus/
├── crates/                   # 11 Rust crates
│   ├── nexus-core/           # State machine, types, protocol, recovery
│   ├── nexus-event-store/    # SQLite + PostgreSQL event stores
│   ├── nexus-rpc/            # JSON-RPC 2.0 codec
│   ├── nexus-security/       # Capability tokens, sandbox
│   ├── nexus-scheduler/      # Local, Docker, K8s schedulers
│   ├── nexus-cli/            # CLI binary
│   ├── nexus-metrics/        # Prometheus metrics
│   ├── nexus-coordinator/    # Multi-agent coordination
│   ├── nexus-message-bus/    # Distributed causal bus
│   ├── nexus-temporal/       # Temporal adapter
│   └── phoenix-tests/        # Acceptance test framework
├── workers/                  # Worker runtimes
│   ├── python-worker/        # Python JSON-RPC worker
│   ├── node-worker/          # Node.js JSON-RPC worker
│   └── rust-worker/          # Rust JSON-RPC worker
├── adapters/                 # Agent interface adapters
│   ├── openclaw/             # OpenClaw gateway
│   └── hermes/               # Hermes CLI
├── sdk/                      # Language SDKs
│   ├── python/               # Python SDK
│   ├── nodejs/               # Node.js SDK
│   └── rust/                 # Rust SDK
├── docs/                     # Protocol, architecture, ADRs
├── fixtures/                 # Golden test fixtures (MessagePack)
├── scripts/                  # Build and demo scripts
├── policies/                 # OPA/Rego policy definitions
├── k8s/                      # Kubernetes deployment configs
├── docker-compose.lite.yml   # Lite mode deployment
├── docker-compose.pro.yml    # Pro mode deployment
└── td.md                     # Frozen technical specification
```

---

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-MIT) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
