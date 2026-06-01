# Nexus Runtime v1.0
## Product Requirements Document

**Classification:** Public — Open Source (MIT + Apache 2.0 Dual License)  
**Version:** 1.0.0-FINAL  
**Date:** 2026-05-30  
**Status:** Architecture Freeze — Ready for Implementation  
**Owner:** Architecture Team  
**Distribution:** Engineering, Product, Security, Developer Relations

---

## 1. Executive Summary

### 1.1 Problem Statement

Modern AI agent systems have achieved remarkable capability in reasoning, tool use, and multi-step execution. However, the infrastructure layer beneath these agents remains fundamentally unreliable. The dominant failure mode of long-running autonomous execution is not model quality—it is **orchestration state corruption**.

Current systems (OpenClaw, Hermes, and their derivatives) share a common architectural flaw: **orchestration state lives primarily in RAM, implicit in execution order, and non-deterministic in recovery**. When processes crash, laptops sleep, or networks partition, these systems lose context, duplicate side effects, and force users to reconstruct execution from scratch.

This document defines **Nexus Runtime**: a causally-consistent, crash-recoverable, deterministic execution substrate for autonomous agent systems. Nexus is not an agent framework. It is infrastructure that makes agent frameworks reliable.

### 1.2 Product Vision

> **Nexus Runtime transforms agent execution from ephemeral inference loops into durable, auditable, and portable computational processes.**

OpenClaw connects agents to the world. Hermes enables agents to learn. Nexus ensures that agent execution survives arbitrary failure without state corruption, side-effect duplication, or cognitive drift.

### 1.3 Target Users

| Segment | Description | Pain Point |
|---------|-------------|------------|
| **AI Engineers** | Building custom agent systems | Need deterministic recovery after crashes |
| **Agent Framework Authors** | Maintaining OpenClaw, Hermes, LangChain, etc. | Need reliable execution substrate beneath their abstractions |
| **Enterprise DevOps** | Operating agent systems in production | Need audit trails, cost governance, and fault tolerance |
| **End Users** | Using Claude Code, Cursor, Codex, etc. | Need execution continuity across sessions and tools |

### 1.4 Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Recovery Success Rate | 100% | Phoenix test suite: `kill -9` at any step → resume without LLM re-call |
| State Replay Divergence | 0 | Byte-identical state reconstruction from event log |
| Side-Effect Duplication | 0 | No duplicate LLM calls, API calls, or file writes after recovery |
| Recovery Latency | < 2s | Time from process restart to execution continuation |
| Cross-Session Memory Inheritance | > 95% | Causally-linked memories available in resumed sessions |
| Worker Failure Isolation | 100% | Worker crash never corrupts Kernel state |

---

## 2. Market & Competitive Analysis

### 2.1 Existing Solutions

| Product | Category | Strength | Critical Weakness |
|---------|----------|----------|-------------------|
| **OpenClaw** | Multi-channel agent gateway | 15+ platform integrations, 33,000+ community skills | State in RAM; no crash recovery; 20+ CVEs including CVSS 9.6; ClawHavoc supply chain attack |
| **Hermes** | Self-hosted agent runtime | Checkpoint v2, four-layer memory, seven-layer security | Checkpoint is snapshot (not event log); no automatic recovery; self-evaluation unreliable |
| **Temporal** | Durable execution engine | Proven workflow recovery, event-sourced replay | External infrastructure; no agent-specific abstractions; no cross-session memory |
| **LangChain** | Agent orchestration framework | Rapid prototyping, extensive integrations | In-memory state; no persistence guarantees; non-deterministic recovery |
| **AutoGen** | Multi-agent conversation framework | Flexible agent collaboration | No durable state; conversation history is not execution state |

### 2.2 Nexus Differentiation

Nexus occupies a **unique position** in the agent infrastructure stack:

```
Application Layer (OpenClaw, Hermes, Cursor, Claude Code)
        ↑
   Nexus Protocol (causal consistency, event log, state machine)
        ↑
Execution Substrate (SQLite/PostgreSQL/Temporal)
        ↑
Operating System
```

**Nexus is the only system that simultaneously provides:**
1. **Local-first operation** (zero infrastructure dependency in Lite mode)
2. **Deterministic crash recovery** (event-sourced replay, not snapshot restore)
3. **Cross-session execution continuity** (vector-clock-based causal memory)
4. **Multi-agent causal coordination** (happens-before guarantees across sessions)
5. **Side-effect transaction safety** (two-phase intent with automatic compensation)
6. **Worker isolation without container overhead** (JSON-RPC over stdio, capability tokens)

---

## 3. System Architecture

### 3.1 Design Principles (Frozen)

| Principle | Enforcement | Violation Consequence |
|-----------|-------------|----------------------|
| **Event Log is Source of Truth** | All state mutations append to immutable event log | Rejected at code review |
| **State is Materialized View** | `sessions` table is query optimization only; deletable | CI failure if any code reads `sessions` for recovery |
| **transition() is Pure Function** | No async, no IO, no clock, no random in state machine | `cargo test` failure |
| **Workers are Stateless** | No persistent memory, no network access, no direct LLM calls | Worker rejected by Kernel |
| **Deterministic Serialization** | `BTreeMap`, `u64`, `rmp-serde` only; `HashMap`/`f64`/`SystemTime` forbidden | `cargo deny` CI failure |
| **Capability-Based Security** | HMAC-SHA256 tokens, path-canonicalized, least-privilege | Runtime rejection |
| **Phoenix Gate** | No release without passing all 8 recovery invariants | Release blocked |

### 3.2 Five-Layer Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ L5: Agent Interface Adapters                                  │
│    • OpenClaw Gateway Adapter                                 │
│    • Hermes CLI Adapter                                       │
│    • Cursor/Claude Code/Codex IDE Adapters                  │
│    • Custom Web UI / Mobile App                             │
│    → All mutations via Nexus SDK → Kernel API               │
├─────────────────────────────────────────────────────────────┤
│ L4: Nexus Kernel (Rust)                                     │
│    ├── Causal State Machine (pure function, < 1ms)          │
│    ├── Event Store Abstraction (SQLite/PostgreSQL/Temporal)   │
│    ├── Checkpoint & Replay Manager                          │
│    ├── Worker Scheduler (local/Docker/K8s)                  │
│    ├── Entropy Controller (simplified: retry + failure rate)│
│    ├── Side-Effect Guard (2-phase intent protocol)          │
│    └── Cost Governor (AtomicU64, hard ceiling)              │
├─────────────────────────────────────────────────────────────┤
│ L3: Worker Fabric                                           │
│    ├── Python Worker (research, office, data analysis)        │
│    ├── Node.js Worker (code generation, business logic)      │
│    ├── Inline Rust Worker (knowledge base, <100μs)          │
│    └── WASM Sandbox Worker (untrusted community skills)      │
│    → JSON-RPC 2.0 over stdio (NDJSON framing)                │
│    → No ports, no network, no persistent state              │
├─────────────────────────────────────────────────────────────┤
│ L2: Causal Memory & Persistence                             │
│    ├── Event Log (append-only, immutable, signed)           │
│    ├── Memory Graph (causal links, vector clock)              │
│    ├── Derived Vector Index (rebuildable, non-authoritative)  │
│    └── Content Vault (blake3 content-addressed, 2PC)        │
├─────────────────────────────────────────────────────────────┤
│ L1: External Toolchain                                      │
│    ├── MCP Servers (1000+ tools, sandboxed)                 │
│    ├── LLM APIs (OpenAI/Anthropic/local via Kernel proxy)   │
│    └── GitHub / Email / Calendar / Browser (tracked)        │
└─────────────────────────────────────────────────────────────┘
```

### 3.3 Deployment Modes

| Mode | Storage | Scheduler | Use Case | Infrastructure |
|------|---------|-----------|----------|----------------|
| **Lite** | SQLite (WAL) | Local process | Personal development, CLI tools | Zero |
| **Pro** | PostgreSQL | Docker containers | Team collaboration, CI/CD | Docker |
| **Enterprise** | PostgreSQL + Temporal | Kubernetes | Production multi-agent systems | K8s + Temporal |

**Critical:** All three modes share **identical protocol semantics** and **identical state machine behavior**. Performance and scale differ; correctness does not.

---

## 4. Core Features

### 4.1 Causal State Machine

**Description:** A pure-function deterministic automaton that is the sole authority for all state transitions.

**Requirements:**
- FR-4.1.1: `transition(current_state, event, dag) -> next_state` must be pure (no IO, no clock, no random)
- FR-4.1.2: All state mutations flow through `transition()`; no direct database writes outside this function
- FR-4.1.3: Optimistic locking on `version` field prevents concurrent mutation races
- FR-4.1.4: Illegal transitions return `TransitionError::IllegalTransition`; never panic
- FR-4.1.5: State machine must be byte-for-byte reproducible across independent processes (golden fixtures)

**States:**
```
Created → Intake → Planning → Planned → Executing → Checkpointing → [Blocked]
                                                              ↓
                                         Converging ←──┘
                                                              ↓
                                         Reflecting → Completed / Failed / Archived
```

### 4.2 Event-Sourced Persistence

**Description:** All system state is derived from an immutable append-only event log.

**Requirements:**
- FR-4.2.1: Event log is append-only; no UPDATE or DELETE operations permitted
- FR-4.2.2: Each event carries a vector clock (`causal_vector`) for cross-session ordering
- FR-4.2.3: Event payload serialized with `rmp-serde` (MessagePack, StructMap, BigEndian)
- FR-4.2.4: SHA-256 integrity hash on core fields prevents tampering
- FR-4.2.5: `sessions` table is materialized view; system must function after `DROP TABLE sessions;` followed by event replay
- FR-4.2.6: Recovery time < 2s for 1000 events on standard hardware

### 4.3 Deterministic Crash Recovery (Phoenix)

**Description:** The system must survive `kill -9` and resume execution without re-invoking LLMs or duplicating side effects.

**Requirements:**
- FR-4.3.1: Phoenix test suite validates 8 recovery invariants on every release
- FR-4.3.2: LLM calls are cached in event log; recovery replays cached results, never re-calls API
- FR-4.3.3: Side effects classified as Pure/Idempotent/Reversible/Irreversible with class-specific recovery logic
- FR-4.3.4: `demo.sh` script demonstrates kill-9 recovery in < 30 seconds
- FR-4.3.5: Recovery must be automatic; no manual intervention required for standard failures

**Phoenix Invariants:**
| # | Invariant | Verification |
|---|-----------|------------|
| 1 | State Authority | `PRAGMA integrity_check` passes |
| 2 | Checkpoint Identity | Checkpoint ID and step_index survive restart |
| 3 | Replay Integrity | Event replay produces byte-identical state |
| 4 | Artifact Integrity | blake3 hashes of vault files remain valid |
| 5 | Determinism Context | seed/model_version/input_hash preserved |
| 6 | Cost Integrity | No duplicated `llm_calls` or `side_effects` entries |
| 7 | Resume Continuity | Execution resumes from step N+1, not N |
| 8 | Eventual Consistency | All committed transitions reconstructable from `event_log` |

### 4.4 Cross-Session Execution Continuity

**Description:** Agent execution state can be exported, transferred, and resumed across different sessions, tools, and models.

**Requirements:**
- FR-4.4.1: `nexus export <session>` produces `.nexus` file containing event log + memory graph + causal vector
- FR-4.4.2: `nexus import <file>` validates causal consistency and replays events into new session
- FR-4.4.3: Memory inheritance preserves causal links; inherited memories carry provenance metadata
- FR-4.4.4: Cross-model resumption supported (e.g., Claude Code → Codex → Cursor)
- FR-4.4.5: Vector clock merge detects and reports causal conflicts; never silently fabricate consistency

### 4.5 Worker Isolation & Security

**Description:** Workers execute in isolated, stateless, capability-constrained environments.

**Requirements:**
- FR-4.5.1: Workers communicate via JSON-RPC over stdio only; no TCP/UDP sockets, no shared memory
- FR-4.5.2: Workers receive HMAC-SHA256 capability tokens; runtime validates every action against granted capabilities
- FR-4.5.3: Path canonicalization prevents directory traversal (`../` escapes)
- FR-4.5.4: Workers have no direct network access; all LLM/tool calls routed through Kernel proxy
- FR-4.5.5: Sandboxing tiers: Landlock/seccomp (Tier 0) → strict path sandbox (Tier 1) → command audit (Tier 2)
- FR-4.5.6: Worker crash never corrupts Kernel state; scheduler detects dead workers and reschedules tasks

### 4.6 Side-Effect Transaction Protocol

**Description:** All external actions follow two-phase intent to ensure crash consistency.

**Requirements:**
- FR-4.6.1: Phase 1: Record `PENDING` intent in `side_effects` table before execution
- FR-4.6.2: Phase 2: Execute external call via Kernel proxy
- FR-4.6.3: Phase 3: Update to `COMMITTED` with response hash in same transaction as event log append
- FR-4.6.4: Recovery queries external system by `idempotency_key` to determine if effect executed before crash
- FR-4.6.5: Reversible effects (file edits) store compensation data (inverse patch) for automatic rollback
- FR-4.6.6: Irreversible effects (email sent) require human approval for replay after crash

### 4.7 Cost Governance

**Description:** Built-in token and compute budget control with hard ceilings.

**Requirements:**
- FR-4.7.1: Budget tracked per-session, per-user, per-hour, per-day (four-level circuit breaker)
- FR-4.7.2: Currency stored as `u64` cents; no floating-point arithmetic in cost calculations
- FR-4.7.3: LLM calls routed through Kernel proxy; Workers cannot directly access paid APIs
- FR-4.7.4: Budget exhaustion triggers `Blocked` state; requires explicit human approval to continue
- FR-4.7.5: Cost predictability: actual < 1.2x budget at 95th percentile

### 4.8 Entropy Controller (Simplified)

**Description:** Monitor execution health and trigger automatic degradation when instability detected.

**Requirements:**
- FR-4.8.1: Monitor signals: retry rate, worker failure rate, validation divergence
- FR-4.8.2: Thresholds: 0.0-0.3 normal; 0.3-0.5 warning; 0.5-0.7 degradation; 0.7-0.85 halt; 0.85+ circuit breaker
- FR-4.8.3: Degradation actions: reduce parallelism, increase validation, freeze adaptation, trigger human review
- FR-4.8.4: Entropy calculation < 10ms (hot path)

---

## 5. Data Model

### 5.1 Core Entities

**NexusEvent**
| Field | Type | Constraints |
|-------|------|-------------|
| event_id | TEXT PK | `e_{timestamp}_{seq}_{session_id}` |
| event_type | TEXT | ENUM (24 types) |
| session_id | BLOB NOT NULL | 16-byte UUID |
| trace_id | BLOB NOT NULL | 16-byte UUID |
| parent_event_id | TEXT | FK to events |
| causal_vector | TEXT NOT NULL | JSON: `{session_id: count}` |
| payload | BLOB NOT NULL | rmp-serde, zstd compressed |
| payload_hash | TEXT NOT NULL | SHA-256 |
| event_timestamp | INTEGER NOT NULL | Unix millis |
| nonce | TEXT NOT NULL | UUID |
| integrity_hash | TEXT NOT NULL | SHA-256 of core fields |

**NexusState (Materialized)**
| Field | Type | Constraints |
|-------|------|-------------|
| session_id | BLOB PK | 16-byte UUID |
| version | INTEGER | Optimistic lock, increments on mutation |
| status | TEXT | ENUM (14 states) |
| intent_graph | BLOB | rmp-serde |
| execution_frontier | BLOB | rmp-serde |
| memory_refs | BLOB | rmp-serde |
| budget | BLOB | rmp-serde |
| checkpoint_seq | INTEGER | Default 0 |
| created_at | INTEGER | Unix millis |
| updated_at | INTEGER | Unix millis |
| latest_event_id | TEXT | FK to events |

### 5.2 Event Type Catalog

| Event Type | Publisher | Description |
|------------|-----------|-------------|
| INTENT_RECEIVED | L5 Adapter | User input captured |
| INTENT_PARSED | Kernel | Intent graph generated |
| PLAN_PROPOSED | Planner | LLM proposes execution plan |
| PLAN_COMMITTED | Kernel | Plan validated and committed |
| FRONTIER_VALIDATED | Validator | Execution boundary preconditions pass |
| WORKER_DISPATCHED | Scheduler | Worker assigned to task |
| WORKER_STARTED | Worker | Worker begins execution |
| WORKER_CHECKPOINT | Worker | Progress snapshot during execution |
| WORKER_COMPLETED | Worker | Task finished successfully |
| WORKER_FAILED | Worker | Task failed, may retry |
| CONVERGE_STARTED | Kernel | Multi-worker result merge begins |
| CONVERGE_COMPLETE | Kernel | Merge finished |
| REFLECTION_STARTED | Kernel | Post-execution evaluation |
| REFLECTION_COMPLETE | Reflection Engine | Evaluation finished, memory delta produced |
| MEMORY_CONSOLIDATED | Memory Manager | Working memory merged to long-term |
| SIDE_EFFECT_INTENT | Effect Guard | External action intent recorded |
| SIDE_EFFECT_COMMITTED | Effect Guard | External action executed |
| HUMAN_APPROVAL_REQUESTED | Policy Engine | Action blocked pending human review |
| HUMAN_APPROVED | L5 Adapter | Human reviewer approved action |
| SESSION_SUSPENDED | Kernel | Session paused (user or automatic) |
| SESSION_RESUMED | Kernel | Session resumed from checkpoint |
| SESSION_ARCHIVED | Kernel | Session completed and frozen |
| SESSION_MIGRATED | Kernel | Session exported to another runtime |
| POLICY_DECISION | Policy Engine | Governance decision recorded |

---

## 6. API Specification

### 6.1 CLI Interface

```bash
# Session Lifecycle
nexus run <intent> [--model <model>] [--budget <usd>]
nexus resume <session-id> [--from <checkpoint>]
nexus suspend <session-id>
nexus status <session-id>
nexus archive <session-id>

# Cross-Session Portability
nexus export <session-id> --output <file.nexus>
nexus import <file.nexus> [--as <new-session-id>]

# Inspection & Debugging
nexus log <session-id> [--limit <n>] [--since <timestamp>]
nexus inspect <session-id> [--state | --memory | --budget]
nexus diff <session-a> <session-b>

# Worker Management
nexus worker list
nexus worker logs <worker-id>
nexus worker kill <worker-id>

# Governance
nexus budget status
nexus budget set --session <id> --limit <usd>
nexus policy apply <policy.yaml>
```

### 6.2 SDK Interface (Python Example)

```python
from nexus import Runtime, Session

# Initialize runtime (Lite mode: SQLite)
runtime = Runtime(mode="lite", db_path="~/.nexus/events.db")

# Create and execute session
session = runtime.create_session(
    intent="refactor authentication to use JWT",
    model="claude-3.5-sonnet",
    budget_usd=5.00
)

# Execution is automatic; checkpoints happen transparently
result = session.run()

# Crash happens here? No problem.
# On next startup:
resumed = runtime.resume_session(session.id)
assert resumed.checkpoint_seq > session.checkpoint_seq

# Export for cross-tool migration
runtime.export_session(session.id, "auth-refactor.nexus")

# Import in another tool
other_runtime = Runtime(mode="lite")
imported = other_runtime.import_session("auth-refactor.nexus")
assert imported.causal_vector.is_consistent()
```

### 6.3 JSON-RPC Worker Protocol

**Core → Worker:**
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "execute",
  "params": {
    "task_id": "task_...",
    "intent": {"type": "refactor", "target": "auth"},
    "inputs": [{"uri": "vault://...", "blake3": "abc..."}],
    "from_step": 0,
    "capabilities": ["fs:read:/project", "tool:github:pr"]
  }
}
```

**Worker → Core (checkpoint notification):**
```json
{
  "jsonrpc": "2.0",
  "method": "checkpoint",
  "params": {
    "task_id": "task_...",
    "step_index": 3,
    "actions": [{"type": "read_file", "path": "auth.py"}],
    "artifacts": [{"id": "art_...", "uri": "vault://...", "blake3": "def..."}]
  }
}
```

**Worker → Core (result):**
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "status": "completed",
    "artifacts": [{"id": "art_...", "uri": "vault://..."}]
  }
}
```

---

## 7. Security Requirements

### 7.1 Threat Model

| Threat | Likelihood | Impact | Mitigation |
|--------|------------|--------|------------|
| Worker container escape | Low | Critical | Defense in depth: namespaces, seccomp, capabilities, read-only rootfs |
| Kernel compromise | Low | Critical | Minimal attack surface, no external network, code audit |
| Prompt injection via task | Medium | High | Input sanitization, output validation, sandboxed execution |
| Supply chain attack (skills) | Medium | High | Capability tokens, sandbox tiers, no auto-execution of untrusted skills |
| Side-channel data exfiltration | Low | Medium | Network isolation, no shared memory |
| Denial of service | Medium | Medium | Rate limiting, resource quotas, circuit breakers |

### 7.2 Security Principles

- **SR-7.2.1:** Workers execute with least privilege; capability tokens are fine-grained and time-bounded
- **SR-7.2.2:** No secret material (API keys, tokens) accessible to Workers; all external calls proxied through Kernel
- **SR-7.2.3:** Audit trail is immutable; all events cryptographically signed
- **SR-7.2.4:** Sandboxing degrades gracefully; if Landlock unavailable, fall back to seccomp; if seccomp unavailable, fall back to command audit; never refuse execution due to missing sandbox support
- **SR-7.2.5:** Recovery never bypasses security; resumed sessions revalidate all capability tokens

---

## 8. Performance Requirements

| Metric | Target | Measurement Condition |
|--------|--------|----------------------|
| State transition latency | < 1ms | Single event, SQLite WAL |
| Event log append throughput | > 10,000 events/sec | Batch insert, SQLite WAL |
| Recovery time | < 2s | 1000 events, standard SSD |
| Worker spawn latency | < 500ms | Local process, cold start |
| Worker checkpoint latency | < 100ms | Stdio round-trip |
| Memory overhead per session | < 1MB | Excluding artifact content |
| Database size growth | < 100MB/day | Typical developer usage |

---

## 9. Implementation Phases

### Phase 0 — Protocol Freeze (2 weeks)
**Deliverables:**
- Nexus Protocol v1.0 specification document
- Event schema (JSON Schema + Protobuf)
- State machine formal definition
- Serialization specification (rmp-serde profile)
- Golden fixture test vectors

**Gating Criteria:**
- Three independent reviewers confirm protocol self-consistency
- Golden fixtures produce identical bytes on Linux/macOS/Windows

### Phase 1 — Lite Core (4 weeks)
**Deliverables:**
- `nexus-core` crate: state machine + event types
- `nexus-event-store` crate: SQLite implementation
- `nexus-rpc` crate: JSON-RPC codec + canonicalization
- `nexus-security` crate: capability tokens + sandbox stubs
- `phoenix-tests` crate: 8-invariant test harness
- Python Worker template
- CLI: `run`, `resume`, `status`, `log`

**Gating Criteria:**
- `cargo test phoenix_` passes 100%
- `demo.sh` outputs `RECOVERY SUCCESSFUL` in < 30s
- `kill -9` at any step → resume without LLM re-call

### Phase 2 — Pro Adapters (4 weeks)
**Deliverables:**
- PostgreSQL event store adapter
- Docker Worker scheduler
- OpenClaw Gateway Adapter
- Hermes CLI Adapter
- `export`/`import` CLI commands
- Cross-session memory inheritance

**Gating Criteria:**
- Session exported from OpenClaw → imported into Hermes → execution continues
- Causal consistency verified across import boundary

### Phase 3 — Enterprise Scale (6 weeks)
**Deliverables:**
- Temporal workflow adapter
- Kubernetes Worker scheduler
- Distributed causal message bus
- Multi-agent coordination protocol
- Prometheus metrics export

**Gating Criteria:**
- 100 concurrent sessions, 99.9% recovery success
- Cross-node session migration < 5s

### Phase 4 — Ecosystem (8 weeks)
**Deliverables:**
- Python SDK (`pip install nexus-runtime`)
- Node.js SDK (`npm install @nexus/runtime`)
- Rust SDK (crates.io)
- Skill marketplace (formalized submission + sandbox testing)
- Web dashboard (session inspector, event visualizer)
- Documentation site (protocol spec + tutorials + API reference)

**Gating Criteria:**
- 10 external contributors submit PRs
- 3 production deployments documented

---

## 10. Open Source Strategy

### 10.1 Licensing
- **Core Protocol:** MIT License (maximum adoption)
- **Reference Implementation:** Apache 2.0 (patent protection)
- **Documentation:** CC BY 4.0

### 10.2 Governance
- **Benevolent Dictator:** Architecture Team (initial 6 months)
- **Technical Steering Committee:** Formed at 10 active maintainers
- **Decision Process:** ADR (Architecture Decision Record) for major changes
- **Contribution Model:** DCO (Developer Certificate of Origin), no CLA

### 10.3 Ecosystem Incentives
- **Skill Marketplace:** Community skills automatically sandboxed and rated
- **Adapter Bounties:** Grants for IDE/tool adapters (VS Code, JetBrains, Vim)
- **Integration Partners:** Certified compatible with OpenClaw, Hermes, LangChain, AutoGen

---

## 11. Risks & Mitigations

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Temporal Rust SDK delays Enterprise timeline | Medium | High | Maintain SQLite/PostgreSQL path; Temporal is optional |
| Community adoption slower than expected | Medium | High | Focus on OpenClaw/Hermes adapter quality; demonstrate value through migration |
| Performance bottleneck in event replay | Low | High | Benchmark early; implement snapshot optimization if needed |
| Security vulnerability in Worker isolation | Low | Critical | Defense in depth; external security audit before v1.0 |
| Protocol fragmentation (competing standards) | Medium | Medium | Rapid standardization through working code; avoid premature RFC process |

---

## 12. Glossary

| Term | Definition |
|------|------------|
| **Causal Vector** | Vector clock tracking happens-before relationships across sessions |
| **Checkpoint** | Execution progress snapshot stored as replayable actions, not memory dump |
| **Event Log** | Append-only immutable record of all system state changes; source of truth |
| **Effect Guard** | Proxy layer controlling all Worker interactions with external systems |
| **Entropy** | Quantitative measure of runtime instability in cognitive execution |
| **Frontier** | Bounded DAG fragment of execution plan currently being processed |
| **Materialized View** | Query-optimized cache of state derived from event log; non-authoritative |
| **Phoenix** | Acceptance test framework proving crash recovery correctness |
| **Side-Effect Class** | Categorization of external actions: Pure, Idempotent, Reversible, Irreversible |
| **Worker** | Stateless, isolated execution unit; compute fabric, not intelligent actor |

---

## 13. Document History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 0.1 | 2026-05-20 | Architecture Team | Initial system definition from project convergence analysis |
| 0.2 | 2026-05-22 | Architecture Team | Added ADR-001 through ADR-005; frozen design principles |
| 0.3 | 2026-05-24 | Architecture Team | Refined Worker protocol; added Phoenix invariant definitions |
| 0.4 | 2026-05-26 | Architecture Team | Added cross-session continuity; causal memory graph specification |
| 0.5 | 2026-05-28 | Architecture Team | Added deployment modes; Temporal adapter strategy |
| 1.0 | 2026-05-30 | Architecture Team | **FINAL** — Architecture freeze; ready for Phase 0 implementation |

---

## 14. Appendices

### Appendix A: ADR Index
- ADR-001: Deterministic Runtime vs Probabilistic Cognition
- ADR-002: Temporal as Durable Execution Substrate (Enterprise Optional)
- ADR-003: LLM Output as Externalized Events
- ADR-004: Runtime Authority Boundary (LLM Proposes → Runtime Validates → Execution Commits)
- ADR-005: Governance Hot Path vs Cold Path

### Appendix B: Reference Implementations
- wtf: Local-first deterministic orchestration (SQLite, Rust, Phoenix tests)
- Axiom MEP: Temporal-based durable execution prototype
- Makima Kernel: Centralized cognitive execution with entropy control

### Appendix C: Related Work
- OpenClaw: Multi-channel agent gateway (compatibility target)
- Hermes: Self-hosted agent runtime with checkpointing (compatibility target)
- Temporal.io: Durable execution engine (Enterprise substrate option)
- Event Sourcing Pattern: Fowler, 2005; Young, 2010
- Vector Clocks: Lamport, 1978; Mattern, 1989

---

*This document is frozen. Changes require ADR and Technical Steering Committee approval.*

**Nexus Runtime v1.0 — Product Requirements Document**  
*Making agent execution durable, auditable, and portable.*
