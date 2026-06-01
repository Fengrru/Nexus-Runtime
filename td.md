# Nexus Runtime v1.0 — Final Technical Specification

**Classification:** Public — Open Source (MIT + Apache 2.0 Dual License)  
**Version:** 1.0.0-FINAL  
**Date:** 2026-05-30  
**Status:** Architecture Freeze — Implementation Ready  
**Owner:** Architecture Team

---

## Table of Contents
1. [System Overview](#1-system-overview)
2. [Architecture](#2-architecture)
3. [Core Data Models](#3-core-data-models)
4. [State Machine](#4-state-machine)
5. [Event Store](#5-event-store)
6. [Worker Protocol](#6-worker-protocol)
7. [Checkpoint & Recovery](#7-checkpoint--recovery)
8. [Causal Memory System](#8-causal-memory-system)
9. [Side-Effect Transaction Protocol](#9-side-effect-transaction-protocol)
10. [Security Model](#10-security-model)
11. [Entropy Controller](#11-entropy-controller)
12. [Scheduler](#12-scheduler)
13. [Serialization & Determinism](#13-serialization--determinism)
14. [Phoenix Test Framework](#14-phoenix-test-framework)
15. [Performance Engineering](#15-performance-engineering)
16. [Build & Deployment](#16-build--deployment)
17. [Error Handling](#17-error-handling)
18. [Observability](#18-observability)
19. [Implementation Roadmap](#19-implementation-roadmap)
20. [Repository Structure](#20-repository-structure)
21. [Appendices](#21-appendices)

---

## 1. System Overview

### 1.1 Identity Statement
Nexus Runtime is a causally-consistent execution substrate for autonomous agent systems. It is not an agent framework, not a chatbot wrapper, not a cloud SaaS. It is infrastructure that makes agent execution durable, auditable, and portable.

**Core principle:** Event log is the source of truth. State is a materialized view. Workers are stateless. The Kernel owns causality.

### 1.2 Problem Domain
Current agent systems share a critical flaw: orchestration state lives in RAM, implicit in execution order, and non-deterministic in recovery. When processes crash, context is lost, side effects are duplicated, and users must reconstruct execution from scratch.

Nexus solves this by treating agent execution as event-sourced, deterministic state machines with vector-clock-based causal consistency across sessions.

### 1.3 Design Constraints (Frozen)

| Constraint | Enforcement | Violation |
|---|---|---|
| `transition()` is pure function | No async, no IO, no clock, no random | `cargo test` failure |
| Event log is append-only | No UPDATE/DELETE on events table | Runtime panic |
| Deterministic serialization | `BTreeMap`, `u64`, `rmp-serde` only | `cargo deny` CI failure |
| Workers are stateless | No persistent memory, no network, no direct LLM | Kernel rejection |
| Phoenix gate | All 8 invariants pass before release | Release blocked |

---

## 2. Architecture

### 2.1 Layer Model

```
┌─────────────────────────────────────────────────────────────┐
│ L5: Agent Interface Adapters                                  │
│    OpenClaw Gateway / Hermes CLI / Cursor / Claude Code      │
│    → All mutations via Nexus SDK → Kernel API                │
├─────────────────────────────────────────────────────────────┤
│ L4: Nexus Kernel (Rust)                                     │
│    ├── Causal State Machine (pure function, < 1ms)          │
│    ├── Event Store (SQLite/PostgreSQL/Temporal adapter)      │
│    ├── Checkpoint & Replay Manager                          │
│    ├── Worker Scheduler (local/Docker/K8s)                  │
│    ├── Entropy Controller                                   │
│    ├── Side-Effect Guard (2-phase intent)                   │
│    └── Cost Governor (AtomicU64, hard ceiling)              │
├─────────────────────────────────────────────────────────────┤
│ L3: Worker Fabric                                           │
│    ├── Python Worker (research, office)                     │
│    ├── Node.js Worker (code, business)                      │
│    ├── Inline Rust Worker (knowledge base, <100μs)          │
│    └── WASM Sandbox Worker (untrusted skills)               │
│    → JSON-RPC 2.0 over stdio (NDJSON framing)               │
│    → No ports, no network, no persistent state              │
├─────────────────────────────────────────────────────────────┤
│ L2: Causal Memory & Persistence                             │
│    ├── Event Log (append-only, immutable)                   │
│    ├── Memory Graph (causal links, vector clock)            │
│    ├── Derived Vector Index (rebuildable)                   │
│    └── Content Vault (blake3, two-phase commit)             │
├─────────────────────────────────────────────────────────────┤
│ L1: External Toolchain                                      │
│    ├── MCP Servers (sandboxed)                              │
│    ├── LLM APIs (via Kernel proxy)                          │
│    └── GitHub/Email/Calendar/Browser (side-effect tracked)  │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Deployment Modes

| Mode | Storage | Scheduler | Infrastructure | Use Case |
|---|---|---|---|---|
| **Lite** | SQLite (WAL) | Local process | Zero | Personal CLI tools |
| **Pro** | PostgreSQL | Docker containers | Docker | Team collaboration |
| **Enterprise** | PostgreSQL + Temporal | Kubernetes | K8s + Temporal | Production multi-agent |

**Critical invariant:** All three modes share identical protocol semantics and identical state machine behavior. Performance and scale differ; correctness does not.

---

## 3. Core Data Models

### 3.1 Primitives

```rust
// crates/nexus-core/src/types.rs
use std::collections::BTreeMap;
use serde::{Serialize, Deserialize};

/// 16-byte UUID, deterministic serialization
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SessionId(pub [u8; 16]);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskId(pub [u8; 16]);

/// Vector clock for cross-session causal ordering
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CausalVector(pub BTreeMap<<SessionId, u64>);

impl CausalVector {
    pub fn new() -> Self { Self(BTreeMap::new()) }
    
    pub fn increment(&mut self, session_id: SessionId) {
        *self.0.entry(session_id).or_insert(0) += 1;
    }
    
    pub fn merge(&mut self, other: &CausalVector) {
        for (k, v) in &other.0 {
            let entry = self.0.entry(*k).or_insert(0);
            *entry = (*entry).max(*v);
        }
    }
    
    pub fn happened_before(&self, other: &CausalVector) -> bool {
        let mut strictly_less = false;
        for (session, count) in &self.0 {
            let other_count = other.0.get(session).copied().unwrap_or(0);
            if *count > other_count { return false; }
            if *count < other_count { strictly_less = true; }
        }
        strictly_less || self.0.len() < other.0.len()
    }
    
    pub fn is_concurrent(&self, other: &CausalVector) -> bool {
        !self.happened_before(other) && !other.happened_before(self)
    }
}
```

### 3.2 State & Budget

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BudgetState {
    pub budget_limit_cents: u64,   // USD cents, never float
    pub consumed_cents: u64,
    pub token_count: u64,
    pub tool_call_count: u64,
}

impl Default for BudgetState {
    fn default() -> Self {
        Self {
            budget_limit_cents: 500, // $5.00 default
            consumed_cents: 0,
            token_count: 0,
            tool_call_count: 0,
        }
    }
}

impl BudgetState {
    pub fn remaining_cents(&self) -> u64 {
        self.budget_limit_cents.saturating_sub(self.consumed_cents)
    }
    pub fn is_exhausted(&self) -> bool {
        self.consumed_cents >= self.budget_limit_cents
    }
    pub fn add_cost(&mut self, cents: u64, tokens: u64, tool_calls: u64) {
        self.consumed_cents = self.consumed_cents.saturating_add(cents);
        self.token_count = self.token_count.saturating_add(tokens);
        self.tool_call_count = self.tool_call_count.saturating_add(tool_calls);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_interval_ms: u64,
    pub backoff_multiplier: f64,
    pub max_interval_ms: u64,
}

impl RetryPolicy {
    pub fn can_retry(&self, attempts: u32) -> bool {
        attempts < self.max_attempts
    }
}
```

### 3.3 Session Status

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Created,
    Intake,
    Planning,
    Planned,
    Executing,
    Checkpointing,
    Blocked,
    Converging,
    Reflecting,
    Completed,
    Failed,
    Archived,
}
```

### 3.4 NexusState

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NexusState {
    pub session_id: SessionId,
    pub version: u64,
    pub status: SessionStatus,
    pub causal_vector: CausalVector,
    pub intent_graph: IntentGraph,
    pub execution_frontier: Frontier,
    pub memory_refs: Vec<<MemoryRef>,
    pub budget: BudgetState,
    pub checkpoint_seq: u64,
    pub created_at: u64,
    pub last_activity_at: u64,
}
```

### 3.5 IntentGraph & Frontier

```rust
/// Directed Acyclic Graph representing decomposed user intent
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct IntentGraph {
    pub root: TaskId,
    pub nodes: BTreeMap<TaskId, TaskNode>,
    pub edges: Vec<(TaskId, TaskId)>, // (from, to)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskNode {
    pub id: TaskId,
    pub kind: TaskKind,
    pub worker_type: WorkerType,
    pub intent: TaskIntent,
    pub dependencies: Vec<TaskId>,
    pub capabilities: Vec<String>,
    pub side_effect_class: SideEffectClass,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    Action,      // Single executable task
    FanIn,       // Barrier: waits for multiple dependencies
    HumanGate,   // Requires human approval before proceeding
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerType {
    Python,
    NodeJs,
    RustInline,
    WasmSandbox,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskIntent {
    pub action_type: String,
    pub target: String,
    pub parameters: BTreeMap<String, String>,
    pub constraints: Vec<<Constraint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Constraint {
    pub constraint_type: String,
    pub value: String,
}

/// Current executable boundary of the intent graph
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Frontier {
    pub nodes: Vec<TaskId>,       // Tasks ready for execution
    pub blocked: Vec<TaskId>,     // Tasks waiting for dependencies
    pub completed: Vec<TaskId>,    // Tasks finished
}

impl Frontier {
    pub fn empty() -> Self {
        Self { nodes: Vec::new(), blocked: Vec::new(), completed: Vec::new() }
    }
    pub fn has_fan_in(&self, dag: &BTreeMap<TaskId, TaskNode>) -> bool {
        self.nodes.iter().any(|task_id| {
            dag.get(task_id).map(|node| node.kind == TaskKind::FanIn).unwrap_or(false)
        })
    }
}
```

### 3.6 MemoryRef & MemoryGraph

```rust
/// Reference to a memory object (not the full content)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryRef {
    pub memory_id: String,
    pub session_origin: SessionId,
    pub causal_vector_at_creation: CausalVector,
    pub importance_score: u64, // 0-1000 (fixed-point, no float)
}

/// Causal memory graph for cross-session inheritance
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryGraph {
    pub nodes: BTreeMap<String, MemoryNode>,
    pub edges: Vec<<MemoryEdge>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryNode {
    pub id: String,
    pub content: MemoryContent,
    pub embedding: Option<Vec<u8>>,  // Compressed f32 array
    pub causal_context: CausalVector,
    pub importance: u64,             // 0-10000 (fixed-point)
    pub activation: u64,             // 0-10000 (computed)
    pub source_event_id: String,
    pub session_lineage: Vec<<SessionId>,
    pub created_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MemoryContent {
    Text { text: String },
    Structured { data: BTreeMap<String, String> },
    Proposition { 
        subject: String,
        predicate: String,
        object: String,
        confidence: u64,  // 0-10000
    },
    Skill { 
        skill_id: String,
        version: String,
        parameters: BTreeMap<String, String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryEdge {
    pub from: String,
    pub to: String,
    pub edge_type: MemoryEdgeType,
    pub confidence: u64,  // 0-10000
    pub created_at: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryEdgeType {
    DerivesFrom,      // B derived from A
    Contradicts,      // B contradicts A
    Refines,          // B refines A
    Generalizes,      // B generalizes A
    Enables,          // A enables B
    CausedBy,         // B caused by A (external)
    SimilarTo,        // Semantic similarity
    PartOf,           // B is part of A
}
```

### 3.7 ArtifactRef

```rust
/// Immutable content-addressed output produced by task execution
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactRef {
    pub id: String,
    pub kind: ArtifactKind,
    pub uri: String,               // vault://{artifact_id}
    pub blake3: String,            // 64-char hex
    pub size_bytes: u64,
    pub produced_by: TaskId,
    pub created_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ArtifactKind {
    File,          // Text or binary file
    Directory,     // Tar archive
    Json,          // Structured data
    Embedding,     // Vector embedding (f32 array)
    Diff,          // Text diff/patch
    Log,           // Execution log
}
```

---

## 4. State Machine

### 4.1 Event Types

```rust
// crates/nexus-core/src/event.rs

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventType {
    // Intake phase
    IntentReceived { raw_input: String, source: String },
    IntentParsed { intent_graph: IntentGraph },
    
    // Planning phase
    PlanProposed { plan: ExecutionPlan, model: String, prompt_tokens: u64, completion_tokens: u64 },
    PlanCommitted { frontier: Frontier },
    PlanRejected { reason: String },
    
    // Execution phase
    DependenciesMet,
    FrontierValidated { validation_result: ValidationResult },
    WorkerDispatched { worker_id: String, task_id: TaskId, worker_type: WorkerType },
    WorkerStarted { worker_id: String, task_id: TaskId, pid: u32 },
    WorkerCheckpoint { task_id: TaskId, step_index: u64, actions: Vec<Action>, artifacts: Vec<<ArtifactRef> },
    WorkerCompleted { worker_id: String, task_id: TaskId, result: WorkerResult, duration_ms: u64 },
    WorkerFailed { worker_id: String, task_id: TaskId, error: String, error_code: ErrorCode, retry_count: u32 },
    
    // Convergence phase
    ConvergeStarted { task_ids: Vec<TaskId> },
    ConvergeComplete { merged_result: WorkerResult },
    
    // Reflection phase
    ReflectionStarted { checkpoint_seq: u64 },
    ReflectionComplete { evaluation: Evaluation, memory_delta: Vec<<MemoryDelta> },
    MemoryConsolidated { memory_ids: Vec<String> },
    
    // Side effects
    SideEffectIntent { effect: SideEffectIntent },
    SideEffectCommitted { effect_id: String, result_hash: String, committed_at: u64 },
    SideEffectCompensated { effect_id: String, compensation_result: String },
    
    // Governance
    HumanApprovalRequested { action: Action, reason: String, timeout_ms: u64 },
    HumanApproved { approver: String, approved_at: u64 },
    HumanRejected { rejecter: String, reason: String },
    PolicyDecision { policy_id: String, decision: PolicyDecision, latency_ms: u64 },
    
    // Session lifecycle
    SessionSuspended { reason: String },
    SessionResumed { from_checkpoint: u64, inherited_memories: Vec<String> },
    SessionMigrated { from: SessionId, to: SessionId, export_hash: String },
    SessionArchived { reason: String, final_status: SessionStatus },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NexusEvent {
    pub event_id: String,
    pub event_type: EventType,
    pub session_id: SessionId,
    pub trace_id: [u8; 16],
    pub parent_event_id: Option<String>,
    pub causal_vector: CausalVector,
    pub payload: Vec<u8>,
    pub payload_hash: String,
    pub event_timestamp: u64,
    pub nonce: String,
    pub integrity_hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCode { Retryable, Fatal }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryDelta {
    pub operation: MemoryOperation,
    pub memory_ref: MemoryRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryOperation { Add, Update, Remove }
```

### 4.2 transition() — Pure Function

```rust
// crates/nexus-core/src/state_machine.rs

/// PURE FUNCTION — The kernel syscall of Nexus Runtime.
/// 
/// INVARIANTS:
/// - No async/await
/// - No I/O operations
/// - No random number generation
/// - No system clock access
/// - No logging
/// - Deterministic: same inputs → same outputs (byte-identical)
///
/// VIOLATION OF ANY INVARIANT IS A CRITICAL BUG.
pub fn transition(
    current: &NexusState,
    event: &NexusEvent,
    dag: &BTreeMap<TaskId, TaskNode>,
) -> Result<NexusState, TransitionError> {
    // Validate event belongs to this session
    if event.session_id != current.session_id {
        return Err(TransitionError::SessionMismatch);
    }
    
    // Validate causal ordering
    if !is_causally_valid(&current.causal_vector, &event.causal_vector) {
        return Err(TransitionError::CausalViolation);
    }
    
    let mut next = current.clone();
    next.version = current.version.wrapping_add(1);
    next.causal_vector.merge(&event.causal_vector);
    next.last_activity_at = event.event_timestamp;
    
    match (current.status, &event.event_type) {
        // === INTAKE PHASE ===
        (SessionStatus::Created, EventType::IntentReceived { .. }) => {
            next.status = SessionStatus::Intake;
            next.causal_vector.increment(current.session_id);
            Ok(next)
        }
        
        (SessionStatus::Intake, EventType::IntentParsed { intent_graph }) => {
            next.status = SessionStatus::Planning;
            next.intent_graph = intent_graph.clone();
            next.causal_vector.increment(current.session_id);
            Ok(next)
        }
        
        // === PLANNING PHASE ===
        (SessionStatus::Planning, EventType::PlanCommitted { frontier }) => {
            next.status = SessionStatus::Planned;
            next.execution_frontier = frontier.clone();
            next.causal_vector.increment(current.session_id);
            Ok(next)
        }
        
        (SessionStatus::Planning, EventType::PlanRejected { .. }) => {
            next.status = SessionStatus::Failed;
            next.causal_vector.increment(current.session_id);
            Ok(next)
        }
        
        // === EXECUTION PHASE ===
        (SessionStatus::Planned, EventType::DependenciesMet) => {
            if current.execution_frontier.has_fan_in(dag) {
                next.status = SessionStatus::Converging;
            } else {
                next.status = SessionStatus::Executing;
            }
            next.causal_vector.increment(current.session_id);
            Ok(next)
        }
        
        (SessionStatus::Executing, EventType::WorkerCheckpoint { step_index, .. }) => {
            if *step_index <= current.checkpoint_seq {
                return Err(TransitionError::StaleCheckpoint {
                    expected: current.checkpoint_seq + 1,
                    received: *step_index,
                });
            }
            next.status = SessionStatus::Checkpointing;
            next.checkpoint_seq = *step_index;
            next.causal_vector.increment(current.session_id);
            Ok(next)
        }
        
        (SessionStatus::Checkpointing, EventType::WorkerCheckpoint { .. }) => {
            // Multiple checkpoints in sequence; stay in Checkpointing
            next.causal_vector.increment(current.session_id);
            Ok(next)
        }
        
        (SessionStatus::Checkpointing, EventType::WorkerCompleted { .. }) => {
            next.status = SessionStatus::Executing;
            next.causal_vector.increment(current.session_id);
            Ok(next)
        }
        
        (SessionStatus::Executing, EventType::WorkerFailed { error_code, .. }) => {
            match error_code {
                ErrorCode::Retryable => {
                    if current.retry_policy.can_retry(1) {
                        next.status = SessionStatus::Planned;
                    } else {
                        next.status = SessionStatus::Failed;
                    }
                }
                ErrorCode::Fatal => next.status = SessionStatus::Failed,
            }
            next.causal_vector.increment(current.session_id);
            Ok(next)
        }
        
        // === CONVERGENCE PHASE ===
        (SessionStatus::Converging, EventType::ConvergeComplete { .. }) => {
            next.status = SessionStatus::Reflecting;
            next.causal_vector.increment(current.session_id);
            Ok(next)
        }
        
        // === REFLECTION PHASE ===
        (SessionStatus::Reflecting, EventType::ReflectionComplete { memory_delta, .. }) => {
            next.status = SessionStatus::Completed;
            next.memory_refs = merge_memory_refs(&current.memory_refs, memory_delta);
            next.causal_vector.increment(current.session_id);
            Ok(next)
        }
        
        // === GOVERNANCE INTERRUPTIONS ===
        (SessionStatus::Executing, EventType::HumanApprovalRequested { .. }) |
        (SessionStatus::Checkpointing, EventType::HumanApprovalRequested { .. }) => {
            next.status = SessionStatus::Blocked;
            next.causal_vector.increment(current.session_id);
            Ok(next)
        }
        
        (SessionStatus::Blocked, EventType::HumanApproved { .. }) => {
            next.status = SessionStatus::Executing;
            next.causal_vector.increment(current.session_id);
            Ok(next)
        }
        
        (SessionStatus::Blocked, EventType::HumanRejected { .. }) => {
            next.status = SessionStatus::Failed;
            next.causal_vector.increment(current.session_id);
            Ok(next)
        }
        
        // === SESSION LIFECYCLE ===
        (_, EventType::SessionSuspended { .. }) => {
            next.status = SessionStatus::Checkpointing;
            next.causal_vector.increment(current.session_id);
            Ok(next)
        }
        
        (SessionStatus::Checkpointing, EventType::SessionResumed { inherited_memories, .. }) => {
            let mut new_memories = current.memory_refs.clone();
            new_memories.extend(inherited_memories.iter().map(|m| MemoryRef {
                memory_id: m.clone(),
                session_origin: current.session_id,
                causal_vector_at_creation: current.causal_vector.clone(),
                importance_score: 500,
            }));
            next.memory_refs = new_memories;
            next.status = SessionStatus::Executing;
            next.causal_vector.increment(current.session_id);
            Ok(next)
        }
        
        (_, EventType::SessionArchived { final_status, .. }) => {
            next.status = *final_status;
            next.causal_vector.increment(current.session_id);
            Ok(next)
        }
        
        // === ILLEGAL TRANSITIONS ===
        _ => Err(TransitionError::IllegalTransition {
            from: format!("{:?}", current.status),
            event: format!("{:?}", event.event_type),
        }),
    }
}

fn is_causally_valid(current: &CausalVector, event: &CausalVector) -> bool {
    for (session_id, current_count) in &current.0 {
        let event_count = event.0.get(session_id).copied().unwrap_or(0);
        if event_count < *current_count { return false; }
    }
    true
}

fn merge_memory_refs(current: &[MemoryRef], delta: &[MemoryDelta]) -> Vec<<MemoryRef> {
    let mut result = current.to_vec();
    for d in delta {
        match d.operation {
            MemoryOperation::Add => {
                if !result.iter().any(|m| m.memory_id == d.memory_ref.memory_id) {
                    result.push(d.memory_ref.clone());
                }
            }
            MemoryOperation::Update => {
                if let Some(idx) = result.iter().position(|m| m.memory_id == d.memory_ref.memory_id) {
                    result[idx] = d.memory_ref.clone();
                }
            }
            MemoryOperation::Remove => {
                result.retain(|m| m.memory_id != d.memory_ref.memory_id);
            }
        }
    }
    result.sort_by(|a, b| b.importance_score.cmp(&a.importance_score));
    result
}
```

### 4.3 State Transition Diagram

```
                         +-----------+
                         |  CREATED  |
                         +-----+-----+
                               |
                               | INTENT_RECEIVED
                               v
                         +-----------+
                         |  INTAKE   |
                         +-----+-----+
                               |
                               | INTENT_PARSED
                               v
                         +-----------+
                         | PLANNING  |
                         +-----+-----+
                               |
              +----------------+----------------+
              |                                 |
              | PLAN_COMMITTED                  | PLAN_REJECTED
              v                                 v
        +-----------+                     +-----------+
        |  PLANNED  |                     |   FAILED  |
        +-----+-----+                     +-----------+
              |
              | DependenciesMet + has_fan_in
              v
        +-----------+
        | CONVERGING|
        +-----+-----+
              |
              | CONVERGE_COMPLETE
              v
        +-----------+
        | REFLECTING|
        +-----+-----+
              |
              | REFLECTION_COMPLETE
              v
        +-----------+
        | COMPLETED |
        +-----------+

        PLANNED → DependenciesMet (no fan_in) → EXECUTING

        EXECUTING → WORKER_CHECKPOINT → CHECKPOINTING
        CHECKPOINTING → WORKER_CHECKPOINT → CHECKPOINTING
        CHECKPOINTING → WORKER_COMPLETED → EXECUTING
        EXECUTING → WORKER_FAILED (retryable) → PLANNED
        EXECUTING → WORKER_FAILED (fatal) → FAILED

        Any active → HUMAN_APPROVAL_REQUESTED → BLOCKED
        BLOCKED → HUMAN_APPROVED → EXECUTING
        BLOCKED → HUMAN_REJECTED → FAILED

        Any → SESSION_SUSPENDED → CHECKPOINTING
        CHECKPOINTING → SESSION_RESUMED → EXECUTING

        Any → SESSION_ARCHIVED → ARCHIVED (or FAILED/COMPLETED)
```

### 4.4 TransitionError

```rust
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub enum TransitionError {
    #[error("Session mismatch: event for {event} applied to session {current}")]
    SessionMismatch { current: String, event: String },
    
    #[error("Causal violation: event vector not monotonic")]
    CausalViolation,
    
    #[error("Stale checkpoint: expected {expected}, received {received}")]
    StaleCheckpoint { expected: u64, received: u64 },
    
    #[error("Illegal transition from {from} via {event}")]
    IllegalTransition { from: String, event: String },
    
    #[error("Budget exceeded: {consumed}/{limit}")]
    BudgetExceeded { consumed: u64, limit: u64 },
    
    #[error("Capability denied: {capability}")]
    CapabilityDenied { capability: String },
    
    #[error("Worker not found: {worker_id}")]
    WorkerNotFound { worker_id: String },
    
    #[error("Timeout")]
    Timeout,
}
```

---

## 5. Event Store

### 5.1 SQLite Schema (Lite/Pro Default)

```sql
-- File: crates/nexus-event-store/schema.sql
-- Target: SQLite 3.45+
-- Mode: WAL (Write-Ahead Logging)

PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;
PRAGMA temp_store=MEMORY;
PRAGMA mmap_size=268435456;  -- 256MB memory-mapped I/O
PRAGMA page_size=4096;
PRAGMA cache_size=-64000;     -- 64MB page cache

-- ============================================================================
-- EVENTS: Immutable event log (SOURCE OF TRUTH)
-- ============================================================================
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    session_id BLOB NOT NULL,
    trace_id BLOB NOT NULL,
    parent_event_id TEXT,
    causal_vector TEXT NOT NULL,
    payload BLOB NOT NULL,
    payload_hash TEXT NOT NULL,
    event_timestamp INTEGER NOT NULL,
    nonce TEXT NOT NULL,
    integrity_hash TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_events_session_time 
    ON events(session_id, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_events_trace 
    ON events(trace_id);
CREATE INDEX IF NOT EXISTS idx_events_type 
    ON events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_parent 
    ON events(parent_event_id);

-- ============================================================================
-- SESSIONS: Materialized state view (DERIVED, REBUILDABLE)
-- ============================================================================
CREATE TABLE IF NOT EXISTS sessions (
    session_id BLOB PRIMARY KEY,
    version INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL,
    intent_graph BLOB NOT NULL,
    execution_frontier BLOB NOT NULL,
    memory_refs BLOB NOT NULL,
    budget BLOB NOT NULL,
    checkpoint_seq INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    latest_event_id TEXT NOT NULL REFERENCES events(event_id)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_sessions_status 
    ON sessions(status);

-- ============================================================================
-- SIDE_EFFECTS: Two-phase intent tracking
-- ============================================================================
CREATE TABLE IF NOT EXISTS side_effects (
    id BLOB PRIMARY KEY,
    session_id BLOB NOT NULL,
    event_id TEXT NOT NULL REFERENCES events(event_id),
    idempotency_key TEXT NOT NULL,
    effect_class TEXT NOT NULL 
        CHECK(effect_class IN ('PURE', 'IDEMPOTENT', 'REVERSIBLE', 'IRREVERSIBLE')),
    status TEXT NOT NULL 
        CHECK(status IN ('PENDING', 'COMMITTED', 'COMPENSATED', 'FAILED')),
    request_payload BLOB NOT NULL,
    request_hash TEXT NOT NULL,
    response_payload BLOB,
    response_hash TEXT,
    compensation_data BLOB,
    committed_at INTEGER,
    UNIQUE(session_id, idempotency_key)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_side_effects_session 
    ON side_effects(session_id, status);
CREATE INDEX IF NOT EXISTS idx_side_effects_idempotency 
    ON side_effects(idempotency_key);

-- ============================================================================
-- RESOURCE_LOCKS: Exclusive/shared resource management
-- ============================================================================
CREATE TABLE IF NOT EXISTS resource_locks (
    resource_id TEXT PRIMARY KEY,
    owner_session BLOB NOT NULL REFERENCES sessions(session_id),
    owner_task BLOB,
    mode TEXT NOT NULL CHECK(mode IN ('EXCLUSIVE', 'SHARED')),
    acquired_at INTEGER NOT NULL,
    lease_expiry INTEGER,
    generation INTEGER NOT NULL DEFAULT 1
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_locks_owner 
    ON resource_locks(owner_session);
CREATE INDEX IF NOT EXISTS idx_locks_expiry 
    ON resource_locks(lease_expiry);

-- ============================================================================
-- LLM_CALLS: Audit trail for all LLM invocations
-- ============================================================================
CREATE TABLE IF NOT EXISTS llm_calls (
    request_id TEXT PRIMARY KEY,
    session_id BLOB NOT NULL,
    event_id TEXT NOT NULL REFERENCES events(event_id),
    model TEXT NOT NULL,
    prompt_hash TEXT NOT NULL,
    response_hash TEXT,
    input_tokens INTEGER,
    output_tokens INTEGER,
    cost_usd_cents INTEGER,
    status TEXT NOT NULL,
    created_at INTEGER NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_llm_calls_session 
    ON llm_calls(session_id, created_at);
CREATE INDEX IF NOT EXISTS idx_llm_calls_model 
    ON llm_calls(model);

-- ============================================================================
-- ARTIFACT_REFS: Content-addressed artifact registry
-- ============================================================================
CREATE TABLE IF NOT EXISTS artifact_refs (
    id BLOB PRIMARY KEY,
    kind TEXT NOT NULL,
    uri TEXT NOT NULL,
    blake3 TEXT NOT NULL,
    size INTEGER NOT NULL,
    produced_by_session BLOB NOT NULL,
    produced_by_event TEXT NOT NULL REFERENCES events(event_id),
    status TEXT NOT NULL,
    created_at INTEGER NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_artifacts_session 
    ON artifact_refs(produced_by_session);
CREATE INDEX IF NOT EXISTS idx_artifacts_blake3 
    ON artifact_refs(blake3);

-- ============================================================================
-- MEMORY_GRAPH: Causal memory relationships
-- ============================================================================
CREATE TABLE IF NOT EXISTS memory_graph (
    memory_id TEXT PRIMARY KEY,
    session_origin BLOB NOT NULL,
    causal_vector TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    content_uri TEXT NOT NULL,
    importance_score INTEGER NOT NULL, -- 0-1000
    activation_score INTEGER NOT NULL, -- 0-1000
    created_at INTEGER NOT NULL,
    last_accessed_at INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS memory_edges (
    from_memory TEXT NOT NULL REFERENCES memory_graph(memory_id),
    to_memory TEXT NOT NULL REFERENCES memory_graph(memory_id),
    edge_type TEXT NOT NULL 
        CHECK(edge_type IN ('derives_from', 'contradicts', 'refines', 
                           'generalizes', 'enables', 'caused_by')),
    confidence INTEGER NOT NULL, -- 0-1000
    PRIMARY KEY (from_memory, to_memory, edge_type)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_memory_edges_from 
    ON memory_edges(from_memory);
CREATE INDEX IF NOT EXISTS idx_memory_edges_to 
    ON memory_edges(to_memory);
```

### 5.2 EventStore Trait

```rust
// crates/nexus-event-store/src/store.rs

#[async_trait::async_trait]
pub trait EventStore: Send + Sync + 'static {
    /// Append event to log (atomic, durable)
    async fn append_event(&self, event: &NexusEvent) -> Result<(), StoreError>;
    
    /// Get events for session, ordered by timestamp
    async fn get_events(
        &self,
        session_id: SessionId,
        since: Option<u64>,
    ) -> Result<Vec<NexusEvent>, StoreError>;
    
    /// Get single event by ID
    async fn get_event(&self, event_id: &str) -> Result<Option<NexusEvent>, StoreError>;
    
    /// Get latest state for session (materialized view)
    async fn get_state(&self, session_id: SessionId) -> Result<Option<NexusState>, StoreError>;
    
    /// Update materialized state (optimistic locking)
    async fn update_state(
        &self,
        state: &NexusState,
        expected_version: u64,
    ) -> Result<bool, StoreError>;
    
    /// Record side-effect intent (Phase 1)
    async fn record_side_effect_intent(
        &self,
        intent: &SideEffectIntent,
    ) -> Result<(), StoreError>;
    
    /// Commit side-effect (Phase 3)
    async fn commit_side_effect(
        &self,
        id: &[u8],
        response_hash: &str,
    ) -> Result<(), StoreError>;
    
    /// Acquire resource lock
    async fn acquire_lock(
        &self,
        resource_id: &str,
        session_id: SessionId,
        mode: LockMode,
    ) -> Result<bool, StoreError>;
    
    /// Release resource lock
    async fn release_lock(
        &self,
        resource_id: &str,
        session_id: SessionId,
    ) -> Result<bool, StoreError>;
    
    /// Record LLM call for audit
    async fn record_llm_call(&self, call: &LlmCallRecord) -> Result<(), StoreError>;
    
    /// Register artifact reference
    async fn register_artifact(&self, artifact: &ArtifactRef) -> Result<(), StoreError>;
    
    /// Health check
    async fn health_check(&self) -> Result<(), StoreError>;
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),
    
    #[error("Optimistic lock conflict: expected version {expected}, found {found}")]
    OptimisticLockConflict { expected: u64, found: u64 },
    
    #[error("Integrity check failed: {0}")]
    IntegrityCheckFailed(String),
    
    #[error("Serialization error: {0}")]
    SerializationError(String),
    
    #[error("Event not found: {0}")]
    EventNotFound(String),
    
    #[error("Store is read-only")]
    ReadOnly,
}
```

### 5.3 SQLite Implementation

```rust
// crates/nexus-event-store/src/sqlite.rs

use sqlx::{sqlite::SqlitePoolOptions, Pool, Sqlite};

pub struct SqliteEventStore {
    pool: Pool<<Sqlite>,
    write_lock: tokio::sync::Mutex<<()>, // Single writer guarantee
}

impl SqliteEventStore {
    pub async fn new(database_url: &str) -> Result<Self, StoreError> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1) // Single writer
            .connect(database_url)
            .await
            .map_err(|e| StoreError::ConnectionFailed(e.to_string()))?;
        
        sqlx::query(include_str!("../schema.sql"))
            .execute(&pool)
            .await
            .map_err(|e| StoreError::ConnectionFailed(e.to_string()))?;
        
        Ok(Self {
            pool,
            write_lock: tokio::sync::Mutex::new(()),
        })
    }
}

#[async_trait::async_trait]
impl EventStore for SqliteEventStore {
    async fn append_event(&self, event: &NexusEvent) -> Result<(), StoreError> {
        let _guard = self.write_lock.lock().await;
        let mut tx = self.pool.begin().await
            .map_err(|e| StoreError::ConnectionFailed(e.to_string()))?;
        
        let payload_bytes = rmp_serde::to_vec(&event.payload)
            .map_err(|e| StoreError::SerializationError(e.to_string()))?;
        
        sqlx::query(
            r#"INSERT INTO events (
                event_id, event_type, session_id, trace_id, parent_event_id,
                causal_vector, payload, payload_hash, event_timestamp,
                nonce, integrity_hash
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)"#
        )
        .bind(&event.event_id)
        .bind(event.event_type.as_str())
        .bind(event.session_id.as_bytes().as_slice())
        .bind(&event.trace_id[..])
        .bind(&event.parent_event_id)
        .bind(event.causal_vector.to_canonical())
        .bind(&payload_bytes)
        .bind(&event.payload_hash)
        .bind(event.event_timestamp as i64)
        .bind(&event.nonce)
        .bind(&event.integrity_hash)
        .execute(&mut *tx)
        .await
        .map_err(|e| StoreError::ConnectionFailed(e.to_string()))?;
        
        tx.commit().await
            .map_err(|e| StoreError::ConnectionFailed(e.to_string()))?;
        
        Ok(())
    }
    
    async fn get_events(
        &self,
        session_id: SessionId,
        since: Option<u64>,
    ) -> Result<Vec<NexusEvent>, StoreError> {
        let rows = if let Some(since_ts) = since {
            sqlx::query_as::<_, SqliteEventRow>(
                "SELECT * FROM events 
                 WHERE session_id = ? AND event_timestamp > ?
                 ORDER BY event_timestamp, event_id"
            )
            .bind(session_id.as_bytes().as_slice())
            .bind(since_ts as i64)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query_as::<_, SqliteEventRow>(
                "SELECT * FROM events 
                 WHERE session_id = ?
                 ORDER BY event_timestamp, event_id"
            )
            .bind(session_id.as_bytes().as_slice())
            .fetch_all(&self.pool)
            .await
        }.map_err(|e| StoreError::ConnectionFailed(e.to_string()))?;
        
        rows.into_iter()
            .map(|r| r.to_nexus_event())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| StoreError::SerializationError(e.to_string()))
    }
    
    // ... additional trait implementations
}
```

---

## 6. Worker Protocol

### 6.1 JSON-RPC 2.0 over stdio

**Transport:** Newline-Delimited JSON (NDJSON) over stdio  
**Encoding:** UTF-8, no byte-order mark  
**Message delimiter:** `\n` (0x0A)  
**Max message size:** 16MB  
**Timeout:** 30s for requests, no timeout for notifications

### 6.2 Message Types

**Core → Worker: execute**
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "execute",
  "params": {
    "task_id": "7461736b5f6578616d706c65",
    "session_id": "736573735f31323334",
    "intent": {
      "action_type": "refactor",
      "target": "authentication",
      "parameters": {
        "strategy": "jwt",
        "preserve_backward_compat": "true"
      },
      "constraints": [
        {"type": "preserve_api", "value": "true"},
        {"type": "test_coverage", "value": ">80%"}
      ]
    },
    "inputs": [
      {
        "artifact_ref": {
          "id": "6172745f35363738",
          "uri": "vault://artifacts/abc123",
          "blake3": "a3f5c8e2d1b4...",
          "size_bytes": 4096
        }
      }
    ],
    "from_step": 0,
    "capabilities": [
      "fs:read:/project/src",
      "fs:write:/project/src/auth",
      "tool:github:pr:create",
      "llm:inference:gpt-4o-mini"
    ],
    "timeout_ms": 300000,
    "token_budget": 100000
  }
}
```

**Core → Worker: cancel**
```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "method": "cancel",
  "params": {
    "task_id": "7461736b5f6578616d706c65",
    "reason": "user_request",
    "timeout_ms": 5000
  }
}
```

**Worker → Core: checkpoint (notification)**
```json
{
  "jsonrpc": "2.0",
  "method": "checkpoint",
  "params": {
    "task_id": "7461736b5f6578616d706c65",
    "step_index": 3,
    "actions": [
      {
        "type": "read_file",
        "path": "/project/src/auth.py",
        "artifact": {
          "id": "6172745f39616263",
          "uri": "vault://artifacts/def456",
          "blake3": "b7e9a1f3c2d8...",
          "size_bytes": 2048
        }
      },
      {
        "type": "edit_file",
        "path": "/project/src/auth.py",
        "search": "def authenticate_session(request):",
        "replace": "def authenticate_jwt(request):",
        "artifact": {
          "id": "6172745f64656630",
          "uri": "vault://artifacts/ghi789",
          "blake3": "c8f0b2e4d3a9...",
          "size_bytes": 2100
        }
      }
    ],
    "progress_percent": 42
  }
}
```

**Worker → Core: progress (notification)**
```json
{
  "jsonrpc": "2.0",
  "method": "progress",
  "params": {
    "task_id": "7461736b5f6578616d706c65",
    "percent": 43,
    "current_step": "analyzing_dependencies",
    "sub_steps": [
      {"name": "read_auth_module", "status": "completed"},
      {"name": "identify_session_usage", "status": "in_progress"},
      {"name": "generate_jwt_replacement", "status": "pending"}
    ]
  }
}
```

**Worker → Core: result (response)**
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "status": "completed",
    "artifacts": [
      {
        "id": "6172745f66696e616c",
        "uri": "vault://artifacts/jkl012",
        "blake3": "d1a3c5f7e4b0...",
        "size_bytes": 4096,
        "kind": "diff",
        "metadata": {
          "files_changed": 3,
          "lines_added": 127,
          "lines_removed": 89
        }
      }
    ],
    "metrics": {
      "duration_ms": 45000,
      "tokens_consumed": 15000,
      "cost_cents": 45
    }
  }
}
```

**Worker → Core: error (response)**
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "error": {
    "code": -32001,
    "message": "Capability denied: fs:write:/etc/passwd",
    "data": {
      "requested_capability": "fs:write:/etc/passwd",
      "granted_capabilities": ["fs:read:/project/src", "fs:write:/project/src"]
    }
  }
}
```

### 6.3 Error Codes

| Code | Name | Description |
|---|---|---|
| -32700 | ParseError | Invalid JSON |
| -32600 | InvalidRequest | JSON-RPC request malformed |
| -32601 | MethodNotFound | Unknown method |
| -32602 | InvalidParams | Parameter validation failed |
| -32603 | InternalError | Worker internal error |
| -32001 | CapabilityDenied | Action exceeds granted capabilities |
| -32002 | BudgetExceeded | Cost or token budget exhausted |
| -32003 | Timeout | Execution exceeded timeout |
| -32004 | Cancelled | Execution cancelled by user |
| -32005 | SandboxViolation | Attempted sandbox escape |

### 6.4 Capability Token Format

```rust
// crates/nexus-security/src/capability.rs

/// HMAC-SHA256 signed capability token
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityToken {
    pub version: u8,
    pub scope: CapabilityScope,
    pub session_id: SessionId,
    pub task_id: TaskId,
    pub expires_at: u64,
    pub issued_at: u64,
    pub signature: Vec<u8>, // HMAC-SHA256
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapabilityScope {
    FsRead { path: String },
    FsWrite { path: String },
    ToolCall { tool_name: String, action: String },
    LlmInference { model: String },
    NetworkRead { host: String },
    NetworkWrite { host: String },
    SystemExec { command_pattern: String },
}

impl CapabilityToken {
    pub fn issue(
        signing_key: &[u8],
        scope: CapabilityScope,
        session_id: SessionId,
        task_id: TaskId,
        expires_at: u64,
    ) -> Self {
        let issued_at = now_millis();
        let mut token = Self {
            version: 1,
            scope,
            session_id,
            task_id,
            expires_at,
            issued_at,
            signature: Vec::new(),
        };
        token.sign(signing_key);
        token
    }
    
    fn sign(&mut self, signing_key: &[u8]) {
        let message = self.signing_message();
        let mut mac = HmacSha256::new_from_slice(signing_key)
            .expect("HMAC can take key of any size");
        mac.update(&message);
        self.signature = mac.finalize().into_bytes().to_vec();
    }
    
    pub fn verify(&self, signing_key: &[u8]) -> Result<(), CapabilityError> {
        if now_millis() > self.expires_at {
            return Err(CapabilityError::Expired);
        }
        if self.version != 1 {
            return Err(CapabilityError::UnsupportedVersion);
        }
        
        let message = self.signing_message();
        let mut mac = HmacSha256::new_from_slice(signing_key)
            .map_err(|_| CapabilityError::InvalidKey)?;
        mac.update(&message);
        mac.verify_slice(&self.signature)
            .map_err(|_| CapabilityError::InvalidSignature)?;
        
        Ok(())
    }
    
    pub fn permits(&self, requested: &CapabilityScope) -> bool {
        match (&self.scope, requested) {
            (CapabilityScope::FsRead { path: granted }, CapabilityScope::FsRead { path: req }) => {
                let granted_canon = canonicalize_path(granted);
                let req_canon = canonicalize_path(req);
                req_canon.starts_with(&granted_canon)
            }
            (CapabilityScope::FsWrite { path: granted }, CapabilityScope::FsWrite { path: req }) => {
                let granted_canon = canonicalize_path(granted);
                let req_canon = canonicalize_path(req);
                req_canon.starts_with(&granted_canon)
            }
            (CapabilityScope::ToolCall { tool_name: gt, action: ga },
             CapabilityScope::ToolCall { tool_name: rt, action: ra }) => {
                gt == rt && (ga == "*" || ga == ra)
            }
            (CapabilityScope::LlmInference { model: gm },
             CapabilityScope::LlmInference { model: rm }) => {
                gm == "*" || gm == rm
            }
            _ => false,
        }
    }
    
    fn signing_message(&self) -> Vec<u8> {
        format!(
            "{}:{}:{}:{}:{}:{}",
            self.version,
            self.scope.to_string(),
            hex::encode(self.session_id.as_bytes()),
            hex::encode(self.task_id.0),
            self.expires_at,
            self.issued_at
        ).into_bytes()
    }
}

/// Prevent directory traversal attacks
fn canonicalize_path(path: &str) -> String {
    let path = std::path::Path::new(path);
    let mut result = std::path::PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::Normal(c) => result.push(c),
            std::path::Component::RootDir => result.push("/"),
            _ => {} // Ignore . and ..
        }
    }
    result.to_string_lossy().into_owned()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapabilityError {
    Expired,
    InvalidSignature,
    InvalidKey,
    UnsupportedVersion,
    InsufficientPermission,
}
```

---

## 7. Checkpoint & Recovery

### 7.1 Checkpoint Structure

```rust
// crates/nexus-core/src/checkpoint.rs

/// Checkpoint is not a memory dump. It is a replayable action log.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Checkpoint {
    pub checkpoint_id: String,
    pub session_id: SessionId,
    pub step_index: u64,
    pub total_actions: u64,
    
    /// Actions to replay from this checkpoint forward
    pub replay_actions: Vec<<ReplayAction>,
    
    /// Artifact references produced up to this checkpoint
    pub artifact_refs: Vec<<ArtifactRef>,
    
    /// External handle reacquisition metadata
    pub handle_registry: Vec<<HandleRecord>,
    
    /// Deterministic execution context
    pub determinism_context: DeterminismContext,
    
    pub created_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReplayAction {
    ReadFile {
        path: String,
        expected_hash: String,
    },
    EditFile {
        path: String,
        search: String,
        replace: String,
        expected_count: u32,
    },
    RunCommand {
        command: String,
        args: Vec<String>,
        env: BTreeMap<String, String>,
    },
    LlmCall {
        request_id: String,
        model: String,
        prompt_hash: String,
        response_artifact: ArtifactRef,
    },
    McpInvoke {
        capability: String,
        args_hash: String,
        result_artifact: Option<<ArtifactRef>,
    },
    GitCommit {
        message: String,
        files: Vec<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HandleRecord {
    pub handle_type: String,
    pub reacquire_command: String,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeterminismContext {
    pub seed: u64,
    pub model_version: String,
    pub input_hash: String,
    pub checkpoint_format_version: u16,
    pub worker_type: WorkerType,
}
```

### 7.2 Recovery Algorithm

```rust
// crates/nexus-core/src/recovery.rs

pub struct RecoveryManager<S: EventStore> {
    store: S,
    vault: Vault,
}

impl<S: EventStore> RecoveryManager<S> {
    pub async fn recover_session(&self, session_id: &SessionId) -> Result<<RecoveryResult, RecoveryError> {
        let mut report = RecoveryReport::default();
        
        // Step 1: Integrity check
        self.store.health_check().await
            .map_err(|e| RecoveryError::StoreCorrupted(e.to_string()))?;
        report.integrity_check = true;
        
        // Step 2: Load all events for session
        let events = self.store.get_events(*session_id, None).await
            .map_err(|e| RecoveryError::EventLoadFailed(e.to_string()))?;
        
        if events.is_empty() {
            return Err(RecoveryError::SessionNotFound);
        }
        
        // Step 3: Verify causal vector monotonicity
        let mut prev_cv = CausalVector::new();
        for event in &events {
            if !is_monotonic(&prev_cv, &event.causal_vector) {
                return Err(RecoveryError::CausalViolation {
                    event_id: event.event_id.clone(),
                    expected: prev_cv,
                    actual: event.causal_vector.clone(),
                });
            }
            prev_cv.merge(&event.causal_vector);
        }
        report.causal_valid = true;
        
        // Step 4: Replay events through state machine
        let mut state = NexusState::new(*session_id, 0);
        let dag = self.build_dag(&events).await?;
        
        for event in &events {
            state = transition(&state, event, &dag)
                .map_err(|e| RecoveryError::ReplayFailed {
                    event_id: event.event_id.clone(),
                    error: format!("{:?}", e),
                })?;
        }
        report.replay_success = true;
        
        // Step 5: Load and verify checkpoint
        let checkpoint = self.load_latest_checkpoint(session_id).await?;
        if let Some(ref cp) = checkpoint {
            if cp.step_index > state.checkpoint_seq {
                return Err(RecoveryError::CheckpointAheadOfState);
            }
            for art in &cp.artifact_refs {
                self.verify_artifact(art).await?;
            }
        }
        report.artifacts_valid = true;
        
        // Step 6: Verify no duplicated LLM calls
        let llm_count = self.count_llm_calls(session_id).await?;
        let unique_llm = self.count_unique_llm_calls(session_id).await?;
        if llm_count != unique_llm {
            return Err(RecoveryError::DuplicatedLlmCalls);
        }
        report.cost_integrity = true;
        
        // Step 7: Build recovery plan
        let recovery_plan = if state.status == SessionStatus::Executing 
            || state.status == SessionStatus::Checkpointing {
            Some(self.build_recovery_plan(&state, &checkpoint).await?)
        } else {
            None
        };
        
        Ok(RecoveryResult {
            state,
            report,
            recovery_plan,
        })
    }
    
    async fn verify_artifact(&self, art: &ArtifactRef) -> Result<(), RecoveryError> {
        let path = self.vault.resolve(&art.uri);
        let content = tokio::fs::read(&path).await
            .map_err(|e| RecoveryError::ArtifactMissing(art.id.clone(), e.to_string()))?;
        
        let actual_hash = blake3::hash(&content).to_hex().to_string();
        if actual_hash != art.blake3 {
            return Err(RecoveryError::ArtifactCorrupted {
                artifact_id: art.id.clone(),
                expected: art.blake3.clone(),
                actual: actual_hash,
            });
        }
        Ok(())
    }
    
    async fn reacquire_handle(&self, handle: &HandleRecord) -> Result<(), RecoveryError> {
        match handle.handle_type.as_str() {
            "file_lock" => { /* Reacquire file lock */ }
            "api_session" => { /* Refresh API token */ }
            _ => { /* Unknown: log warning, continue */ }
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct RecoveryReport {
    pub integrity_check: bool,
    pub causal_valid: bool,
    pub replay_success: bool,
    pub artifacts_valid: bool,
    pub cost_integrity: bool,
}

#[derive(Debug)]
pub struct RecoveryResult {
    pub state: NexusState,
    pub report: RecoveryReport,
    pub recovery_plan: Option<<RecoveryPlan>,
}

#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error("Store corrupted: {0}")]
    StoreCorrupted(String),
    #[error("Event load failed: {0}")]
    EventLoadFailed(String),
    #[error("Session not found")]
    SessionNotFound,
    #[error("Causal violation at {event_id}: expected {expected:?}, got {actual:?}")]
    CausalViolation { event_id: String, expected: CausalVector, actual: CausalVector },
    #[error("Replay failed at {event_id}: {error}")]
    ReplayFailed { event_id: String, error: String },
    #[error("Artifact missing: {0} ({1})")]
    ArtifactMissing(String, String),
    #[error("Artifact corrupted: {artifact_id} expected {expected}, got {actual}")]
    ArtifactCorrupted { artifact_id: String, expected: String, actual: String },
    #[error("Duplicated LLM calls detected")]
    DuplicatedLlmCalls,
    #[error("Checkpoint ahead of state")]
    CheckpointAheadOfState,
    #[error("Worker spawn failed: {0}")]
    WorkerSpawnFailed(String),
}
```

### 7.3 Resume Protocol

```rust
pub async fn resume_execution(
    store: &dyn EventStore,
    scheduler: &dyn Scheduler,
    session_id: SessionId,
) -> Result<(), ResumeError> {
    // 1. Recover session
    let recovered = recover_session(store, session_id).await?;
    
    // 2. Update materialized state
    store.update_state(&recovered.state, recovered.state.version - 1).await?;
    
    // 3. Reacquire external handles
    if let Some(ref cp) = recovered.recovery_plan {
        for handle in &cp.handle_registry {
            reacquire_handle(handle).await?;
        }
    }
    
    // 4. Spawn worker from resume point
    let worker = scheduler.spawn_worker(WorkerConfig {
        task_id: recovered.state.execution_frontier.nodes[0],
        session_id,
        from_step: recovered.recovery_plan.as_ref().map(|p| p.from_step).unwrap_or(0),
        replay_actions: recovered.recovery_plan.map(|p| p.replay_actions).unwrap_or_default(),
        inputs: recovered.state.execution_frontier.nodes.clone(),
    }).await?;
    
    // 5. Record resume event
    let resume_event = NexusEvent {
        event_id: generate_event_id(),
        event_type: EventType::SessionResumed {
            from_checkpoint: recovered.state.checkpoint_seq,
            inherited_memories: recovered.state.memory_refs.iter()
                .map(|m| m.memory_id.clone())
                .collect(),
        },
        session_id,
        trace_id: generate_trace_id(),
        parent_event_id: None,
        causal_vector: recovered.state.causal_vector.clone(),
        payload: vec![],
        payload_hash: String::new(),
        event_timestamp: now_millis(),
        nonce: generate_nonce(),
        integrity_hash: String::new(),
    };
    store.append_event(&resume_event).await?;
    
    Ok(())
}
```

---

## 8. Causal Memory System

### 8.1 Memory Graph

```rust
// crates/nexus-core/src/memory.rs

impl MemoryGraph {
    /// Query memories by causal relationship
    pub fn query_causal(
        &self,
        from: &str,
        edge_type: Option<<MemoryEdgeType>,
        depth: usize,
    ) -> Vec<&MemoryNode> {
        let mut results = Vec::new();
        let mut visited = BTreeSet::new();
        let mut queue = vec![(from.to_string(), 0)];
        
        while let Some((current, current_depth)) = queue.pop() {
            if current_depth > depth || visited.contains(&current) {
                continue;
            }
            visited.insert(current.clone());
            
            if let Some(node) = self.nodes.get(&current) {
                results.push(node);
            }
            
            for edge in &self.edges {
                if edge.from == current {
                    if let Some(ref et) = edge_type {
                        if edge.edge_type != *et { continue; }
                    }
                    queue.push((edge.to.clone(), current_depth + 1));
                }
            }
        }
        results
    }
    
    /// Compute activation score for memory retrieval
    pub fn compute_activation(
        &self,
        memory_id: &str,
        query_context: &QueryContext,
    ) -> u64 {
        let node = match self.nodes.get(memory_id) {
            Some(n) => n,
            None => return 0,
        };
        
        // Relevance: cosine similarity (if embedding available)
        let relevance = match (&node.embedding, &query_context.embedding) {
            (Some(ne), Some(qe)) => cosine_similarity_u8(ne, qe),
            _ => 5000, // Default mid-range
        };
        
        // Importance: stored value (0-10000)
        let importance = node.importance;
        
        // Recency: time decay
        let age_hours = (query_context.now - node.created_at) / 3600_000;
        let recency = if age_hours < 1 { 10000 } else {
            (10000.0 / (1.0 + age_hours as f64).ln()).min(10000.0) as u64
        };
        
        // Goal alignment
        let goal_alignment = if query_context.active_goals.iter()
            .any(|g| node.content.matches_goal(g)) { 8000 } else { 3000 };
        
        // Causal proximity: graph distance to recent memories
        let causal_proximity = query_context.recent_memories.iter()
            .map(|recent| self.graph_distance(memory_id, recent))
            .min()
            .map(|d| if d == 0 { 10000 } else { 10000 / d as u64 })
            .unwrap_or(5000);
        
        // Weighted combination (weights sum to 10000)
        (relevance * 3000 + importance * 2500 + recency * 2000 
            + goal_alignment * 1500 + causal_proximity * 1000) / 10000
    }
    
    fn graph_distance(&self, from: &str, to: &str) -> usize {
        let mut queue = std::collections::VecDeque::new();
        let mut visited = BTreeSet::new();
        queue.push_back((from.to_string(), 0));
        
        while let Some((current, distance)) = queue.pop_front() {
            if &current == to { return distance; }
            if visited.contains(&current) { continue; }
            visited.insert(current.clone());
            
            for edge in &self.edges {
                if edge.from == current {
                    queue.push_back((edge.to.clone(), distance + 1));
                }
            }
        }
        usize::MAX
    }
}

#[derive(Debug, Clone)]
pub struct QueryContext {
    pub embedding: Option<Vec<u8>>,
    pub active_goals: Vec<String>,
    pub recent_memories: Vec<String>,
    pub now: u64,
}
```

### 8.2 Cross-Session Memory Inheritance

```rust
impl MemoryGraph {
    /// Import memories from another session
    pub fn inherit_memories(
        &mut self,
        source: &MemoryGraph,
        source_session: SessionId,
        causal_vector: &CausalVector,
    ) -> Result<Vec<String>, MemoryError> {
        let mut imported = Vec::new();
        
        for (id, node) in &source.nodes {
            // Check causal compatibility
            match node.causal_context.compare(causal_vector) {
                CausalRelation::Concurrent => {
                    // Potential conflict: skip or merge
                    continue;
                }
                _ => {}
            }
            
            // Clone node with updated lineage
            let mut new_node = node.clone();
            new_node.session_lineage.push(source_session);
            new_node.causal_context.merge(causal_vector);
            
            let new_id = format!("{}:{}", source_session.to_hex(), id);
            self.nodes.insert(new_id.clone(), new_node);
            imported.push(new_id);
        }
        
        Ok(imported)
    }
}
```

---

## 9. Side-Effect Transaction Protocol

### 9.1 Two-Phase Intent

**Phase 1: INTENT**
- Worker proposes action
- Kernel validates preconditions (capability, budget, file hash)
- Kernel INSERTS `side_effects` row with `status=PENDING`
- Kernel appends `SIDE_EFFECT_INTENT` event

**Phase 2: EXECUTION**
- Kernel executes via proxy (Worker has no direct network access)
- External system returns result
- Kernel computes `response_hash`

**Phase 3: COMMIT**
- Kernel UPDATE `side_effects` row with `status=COMMITTED`, `response_hash`
- Kernel appends `SIDE_EFFECT_COMMITTED` event
- Both in same SQLite transaction

### 9.2 Effect Classification

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EffectClass {
    Pure,        // No external effect; fully deterministic
    Idempotent,  // Same input → same output; safe to retry
    Reversible,  // Can be undone with compensation data
    Irreversible,// Cannot be undone; only audit
}

impl EffectClass {
    pub fn recovery_strategy(&self) -> RecoveryStrategy {
        match self {
            EffectClass::Pure => RecoveryStrategy::Replay,
            EffectClass::Idempotent => RecoveryStrategy::Replay,
            EffectClass::Reversible => RecoveryStrategy::Compensate,
            EffectClass::Irreversible => RecoveryStrategy::QueryAndConfirm,
        }
    }
}

#[derive(Debug, Clone)]
pub enum RecoveryStrategy {
    Replay,
    Compensate,
    QueryAndConfirm,
}
```

### 9.3 SideEffectGuard Implementation

```rust
// crates/nexus-core/src/side_effect.rs

pub struct SideEffectGuard<S: EventStore> {
    store: Arc<S>,
    llm_proxy: Arc<LlmProxy>,
    tool_proxy: Arc<ToolProxy>,
}

impl<S: EventStore> SideEffectGuard<S> {
    pub async fn record_intent(
        &self,
        intent: SideEffectIntent,
    ) -> Result<String, EffectError> {
        let idempotency_key = format!("{}:{}", 
            intent.session_id.to_hex(), 
            intent.request_hash
        );
        
        // Check for existing intent (idempotency)
        if let Some(existing) = self.store
            .get_side_effect(intent.session_id, &idempotency_key).await? {
            return Ok(existing.id);
        }
        
        // Validate preconditions
        for precond in &intent.preconditions {
            self.validate_precondition(precond).await?;
        }
        
        // Record PENDING in event log
        let event = NexusEvent {
            event_id: generate_event_id(),
            event_type: EventType::SideEffectIntent { effect: intent.clone() },
            session_id: intent.session_id,
            trace_id: generate_trace_id(),
            parent_event_id: None,
            causal_vector: self.get_current_causal_vector(&intent.session_id).await?,
            payload: rmp_serde::to_vec(&intent)?,
            payload_hash: compute_hash(&rmp_serde::to_vec(&intent)?),
            event_timestamp: now_millis(),
            nonce: generate_nonce(),
            integrity_hash: String::new(),
        };
        
        self.store.append_event(&event).await?;
        Ok(intent.id)
    }
    
    pub async fn execute_and_commit(
        &self,
        effect_id: &str,
    ) -> Result<<EffectResult, EffectError> {
        let record = self.store.get_side_effect_by_id(effect_id).await?
            .ok_or(EffectError::IntentNotFound)?;
        
        if record.status != EffectStatus::Pending {
            return Err(EffectError::AlreadyProcessed);
        }
        
        // Execute via Kernel proxy
        let result = match record.intent.effect_class {
            EffectClass::Pure => self.execute_pure(&record.intent).await,
            EffectClass::Idempotent => self.execute_idempotent(&record.intent).await,
            EffectClass::Reversible => self.execute_reversible(&record.intent).await,
            EffectClass::Irreversible => self.execute_irreversible(&record.intent).await,
        };
        
        let (status, response_hash) = match &result {
            Ok(res) => (EffectStatus::Committed, Some(res.hash.clone())),
            Err(_) => (EffectStatus::Failed, None),
        };
        
        // Commit in same transaction as event log
        let commit_event = NexusEvent {
            event_id: generate_event_id(),
            event_type: EventType::SideEffectCommitted {
                effect_id: effect_id.to_string(),
                result_hash: response_hash.clone().unwrap_or_default(),
                committed_at: now_millis(),
            },
            session_id: record.intent.session_id,
            trace_id: generate_trace_id(),
            parent_event_id: Some(record.intent.id.clone()),
            causal_vector: self.get_current_causal_vector(&record.intent.session_id).await?,
            payload: vec![],
            payload_hash: String::new(),
            event_timestamp: now_millis(),
            nonce: generate_nonce(),
            integrity_hash: String::new(),
        };
        
        self.store.append_event(&commit_event).await?;
        result
    }
    
    pub async fn recover_effect(
        &self,
        effect_id: &str,
    ) -> Result<<RecoveryAction, EffectError> {
        let record = self.store.get_side_effect_by_id(effect_id).await?
            .ok_or(EffectError::IntentNotFound)?;
        
        match record.status {
            EffectStatus::Pending => {
                match record.intent.effect_class {
                    EffectClass::Pure | EffectClass::Idempotent => Ok(RecoveryAction::Replay),
                    EffectClass::Reversible => {
                        if record.compensation_data.is_some() {
                            Ok(RecoveryAction::CompensateAndReplay)
                        } else {
                            Ok(RecoveryAction::Replay)
                        }
                    }
                    EffectClass::Irreversible => Ok(RecoveryAction::QueryExternal),
                }
            }
            EffectStatus::Committed => Ok(RecoveryAction::UseCached),
            EffectStatus::Compensated => Ok(RecoveryAction::Replay),
            EffectStatus::Failed => Ok(RecoveryAction::Retry),
        }
    }
}

#[derive(Debug, Clone)]
pub enum RecoveryAction {
    Replay,
    CompensateAndReplay,
    QueryExternal,
    UseCached,
    Retry,
}

#[derive(Debug, thiserror::Error)]
pub enum EffectError {
    #[error("Intent not found")]
    IntentNotFound,
    #[error("Already processed")]
    AlreadyProcessed,
    #[error("Storage error: {0}")]
    StorageError(String),
    #[error("Precondition failed: {0}")]
    PreconditionFailed(String),
    #[error("Execution failed: {0}")]
    ExecutionFailed(String),
}
```

### 9.4 Compensation Data

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompensationData {
    FileEdit {
        original_content_hash: String,
        original_content_uri: String,
    },
    FileCreate {
        created_path: String,
    },
    Command {
        undo_command: String,
        undo_args: Vec<String>,
    },
    DatabaseTransaction {
        rollback_sql: String,
    },
}
```

### 9.5 Effect Class Recovery Matrix

| Class | Examples | Recovery Action | Compensation |
|---|---|---|---|
| **Pure** | `read_file`, `grep`, `calculate` | Replay from event log | None needed |
| **Idempotent** | `upsert`, `replace_text` | Replay from event log | None needed |
| **Reversible** | `edit_file`, `create_file` | Check if executed; if yes, load compensation data | Inverse patch |
| **Irreversible** | `send_email`, `git push`, `deploy` | Query external system by idempotency key; if confirmed, audit only; if unconfirmed, human approval | None possible |

---

## 10. Security Model

### 10.1 Capability Token Lifecycle

1. **Kernel generates token:**
   - `scope = "fs:write:/project/src"`
   - `session_id = current_session`
   - `task_id = current_task`
   - `expires_at = now() + 3600_000` (1 hour)
   - `signature = HMAC-SHA256(secret, scope || session || task || expires)`

2. **Token passed to Worker** via execute params

3. **Worker includes token** in every action request

4. **Kernel verifies:**
   - a. Signature valid (not forged)
   - b. Not expired
   - c. Session matches
   - d. Task matches
   - e. Scope covers requested action
   - f. Path canonicalized (no `../` escapes)

5. **Token revoked** on task completion or session termination

### 10.2 Sandboxing Tiers

| Tier | Mechanism | Requirements | Fallback |
|---|---|---|---|
| **Tier 0** | Landlock + seccomp-bpf + read-only rootfs | Linux 5.13+, libseccomp | Tier 1 |
| **Tier 1** | seccomp-bpf + strict path whitelist | Linux, libseccomp | Tier 2 |
| **Tier 2** | Command audit + logging | Any POSIX | Continue with warning |

```rust
// crates/nexus-security/src/sandbox.rs

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxTier { Tier0, Tier1, Tier2 }

impl SandboxTier {
    pub fn apply(&self, cmd: &mut Command) -> Result<(), SandboxError> {
        match self {
            SandboxTier::Tier0 => self.apply_tier0(cmd),
            SandboxTier::Tier1 => self.apply_tier1(cmd),
            SandboxTier::Tier2 => self.apply_tier2(cmd),
        }
    }
    
    fn apply_tier0(&self, cmd: &mut Command) -> Result<(), SandboxError> {
        #[cfg(target_os = "linux")]
        {
            if let Ok(landlock) = LandlockSandbox::new() {
                landlock.restrict_fs(cmd)?;
            }
            if let Ok(seccomp) = SeccompFilter::new() {
                seccomp.apply(cmd)?;
            }
        }
        cmd.env("NEXUS_READONLY", "1");
        Ok(())
    }
    
    fn apply_tier1(&self, cmd: &mut Command) -> Result<(), SandboxError> {
        #[cfg(target_os = "linux")]
        {
            if let Ok(seccomp) = SeccompFilter::new() {
                seccomp.apply(cmd)?;
            }
        }
        Ok(())
    }
    
    fn apply_tier2(&self, _cmd: &mut Command) -> Result<(), SandboxError> {
        Ok(())
    }
    
    pub fn best_available() -> Self {
        #[cfg(target_os = "linux")]
        {
            if LandlockSandbox::is_available() { return SandboxTier::Tier0; }
            if SeccompFilter::is_available() { return SandboxTier::Tier1; }
        }
        SandboxTier::Tier2
    }
}
```

### 10.3 Seccomp Profile (Worker)

```json
{
  "defaultAction": "SCMP_ACT_ERRNO",
  "architectures": ["SCMP_ARCH_X86_64", "SCMP_ARCH_AARCH64"],
  "syscalls": [
    {
      "names": [
        "read", "write", "open", "close",
        "mmap", "mprotect", "munmap",
        "brk", "exit", "exit_group",
        "getpid", "gettid", "getuid", "getgid",
        "clock_gettime", "nanosleep",
        "futex", "epoll_create1", "epoll_ctl", "epoll_wait"
      ],
      "action": "SCMP_ACT_ALLOW"
    },
    {
      "names": [
        "socket", "connect", "sendto", "recvfrom",
        "clone", "wait4", "kill", "rt_sigaction"
      ],
      "action": "SCMP_ACT_ALLOW",
      "args": [
        {
          "index": 0,
          "value": 1,
          "op": "SCMP_CMP_EQ"
        }
      ]
    },
    {
      "names": [
        "execve", "execveat", "ptrace",
        "mount", "umount2", "pivot_root",
        "open_by_handle_at", "name_to_handle_at",
        "kexec_load", "kexec_file_load",
        "perf_event_open", "bpf"
      ],
      "action": "SCMP_ACT_KILL"
    }
  ]
}
```

### 10.4 Audit Trail

All security-relevant events are logged immutably:

| Event | Table | Retention |
|---|---|---|
| Capability grant | events (`CAPABILITY_GRANTED`) | Permanent |
| Capability denial | events (`CAPABILITY_DENIED`) | Permanent |
| Sandbox violation | events (`SANDBOX_VIOLATION`) | Permanent |
| Side-effect execution | `side_effects` | Permanent |
| LLM call | `llm_calls` | Permanent |
| Human approval | events (`HUMAN_APPROVED`) | Permanent |
| Policy decision | events (`POLICY_DECISION`) | Permanent |

---

## 11. Entropy Controller

### 11.1 Simplified Implementation (v1.0)

```rust
// crates/nexus-core/src/entropy.rs

/// Simplified entropy controller for MVP
/// Full Makima-style entropy deferred to Phase 3
#[derive(Debug, Clone)]
pub struct EntropyController {
    pub thresholds: EntropyThresholds,
}

#[derive(Debug, Clone, Copy)]
pub struct EntropyThresholds {
    pub warning: f64,      // 0.3
    pub degradation: f64,  // 0.5
    pub halt: f64,         // 0.7
    pub circuit_breaker: f64, // 0.85
}

impl Default for EntropyThresholds {
    fn default() -> Self {
        Self {
            warning: 0.3,
            degradation: 0.5,
            halt: 0.7,
            circuit_breaker: 0.85,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct EntropySignals {
    pub retry_rate: f64,           // retries / total_actions
    pub worker_failure_rate: f64,  // failures / spawns
    pub validation_divergence: f64, // std dev of validator scores
}

impl EntropyController {
    pub fn calculate(&self, signals: &EntropySignals) -> f64 {
        let retry_score = signals.retry_rate.min(1.0);
        let failure_score = signals.worker_failure_rate.min(1.0);
        let divergence_score = signals.validation_divergence.min(1.0);
        
        (retry_score * 0.4 + failure_score * 0.4 + divergence_score * 0.2)
            .min(1.0)
    }
    
    pub fn respond(&self, score: f64) -> Vec<<EntropyAction> {
        if score >= self.circuit_breaker {
            vec![
                EntropyAction::HaltExecution,
                EntropyAction::LockNewTasks,
                EntropyAction::AlertOperator,
            ]
        } else if score >= self.halt {
            vec![
                EntropyAction::ReduceParallelism,
                EntropyAction::TriggerHumanReview,
                EntropyAction::SnapshotCheckpoint,
            ]
        } else if score >= self.degradation {
            vec![
                EntropyAction::FreezeAdaptation,
                EntropyAction::IncreaseValidation,
            ]
        } else if score >= self.warning {
            vec![
                EntropyAction::IncreaseSampling,
                EntropyAction::LogWarning,
            ]
        } else {
            vec![]
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntropyAction {
    IncreaseSampling,
    LogWarning,
    FreezeAdaptation,
    IncreaseValidation,
    ReduceParallelism,
    TriggerHumanReview,
    SnapshotCheckpoint,
    HaltExecution,
    LockNewTasks,
    AlertOperator,
}
```

---

## 12. Scheduler

### 12.1 Topological + Capability-Aware Scheduling

```rust
// crates/nexus-scheduler/src/scheduler.rs

pub struct Scheduler {
    ready_queue: VecDeque<TaskId>,
    lock_table: Arc<<tokio::sync::Mutex<BTreeMap<String, TaskId>>>,
    max_concurrency: usize,
    active_workers: BTreeMap<TaskId, WorkerHandle>,
}

impl Scheduler {
    pub async fn tick(&mut self) -> Vec<TaskId> {
        let mut dispatched = Vec::new();
        let mut locks = self.lock_table.lock().await;
        
        while dispatched.len() < self.max_concurrency 
            && self.active_workers.len() < self.max_concurrency {
            
            let Some(task_id) = self.ready_queue.pop_front() else { break };
            let task = self.load_task(task_id).await;
            
            // Check capability availability
            let can_dispatch = task.required_capabilities.iter().all(|cap| {
                match cap.mode {
                    CapabilityMode::Exclusive => !locks.contains_key(&cap.resource),
                    CapabilityMode::Shared => true, // No limit in v1.0
                }
            });
            
            if can_dispatch {
                for cap in &task.required_capabilities {
                    if cap.mode == CapabilityMode::Exclusive {
                        locks.insert(cap.resource.clone(), task_id);
                    }
                }
                dispatched.push(task_id);
            } else {
                self.ready_queue.push_back(task_id);
                break;
            }
        }
        dispatched
    }
    
    pub async fn release_task(&mut self, task_id: TaskId) {
        let mut locks = self.lock_table.lock().await;
        let to_remove: Vec<String> = locks.iter()
            .filter(|(_, owner)| **owner == task_id)
            .map(|(resource, _)| resource.clone())
            .collect();
        for resource in to_remove {
            locks.remove(&resource);
        }
        self.active_workers.remove(&task_id);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityMode {
    Exclusive,
    Shared,
}
```

---

## 13. Serialization & Determinism

### 13.1 Forbidden Types

| Type | Reason | Replacement |
|---|---|---|
| `HashMap` | Non-deterministic iteration order | `BTreeMap` |
| `HashSet` | Non-deterministic iteration order | `BTreeSet` |
| `f32` | Platform-dependent NaN/inf semantics | `u64` (fixed-point) or `String` |
| `f64` | Platform-dependent NaN/inf semantics | `u64` (fixed-point) or `String` |
| `SystemTime` | Non-deterministic across machines | `u64` (Unix millis) |
| `Instant` | Non-deterministic, non-serializable | `u64` (monotonic counter) |
| `serde_json::Value` | Schema-less, non-deterministic | Strongly-typed structs |

### 13.2 Required Types

| Type | Usage | Rationale |
|---|---|---|
| `BTreeMap<K, V>` | Maps, indexes | Deterministic iteration (sorted keys) |
| `BTreeSet<T>` | Unique collections | Deterministic iteration (sorted) |
| `Vec<T>` | Lists, sequences | Deterministic (insertion order) |
| `u64` | Timestamps, counters, currency | Fixed-width, no platform variance |
| `String` | Text, identifiers | UTF-8, platform-independent |
| `Option<T>` | Nullable values | Explicit null handling |
| `Result<T, E>` | Error handling | Explicit error paths |

### 13.3 Serialization Configuration

```rust
// crates/nexus-core/src/protocol.rs

use rmp_serde::{config::StructMapConfig, Serializer};
use serde::Serialize;

/// Deterministic MessagePack serialization configuration
/// 
/// # Properties
/// - StructMap mode: structs serialized as maps (not arrays)
/// - BigEndian integers: platform-independent byte order
/// - No compression: authority structures stored uncompressed
/// - Golden fixtures enforce byte identity across processes
pub fn serialize_deterministic<T: Serialize>(value: &T) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    let mut buf = Vec::new();
    value.serialize(&mut Serializer::new(&mut buf).with_struct_map())?;
    Ok(buf)
}

pub fn deserialize_deterministic<T: serde::de::DeserializeOwned>(
    bytes: &[u8]
) -> Result<T, rmp_serde::decode::Error> {
    rmp_serde::from_slice(bytes)
}

pub fn compute_hash(bytes: &[u8]) -> String {
    use blake3::Hasher;
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finalize().to_hex().to_string()
}

/// Currency handling: store as u64 cents, never f64
pub struct UsdCents(pub u64);

impl UsdCents {
    pub fn from_float(dollars: f64) -> Self {
        Self((dollars * 100.0).round() as u64)
    }
    pub fn to_float(&self) -> f64 {
        self.0 as f64 / 100.0
    }
    pub fn add(&self, other: UsdCents) -> Self {
        Self(self.0 + other.0)
    }
    pub fn subtract(&self, other: UsdCents) -> Option<Self> {
        self.0.checked_sub(other.0).map(Self)
    }
}
```

### 13.4 Clippy Configuration

```rust
// In each crate's lib.rs or via .clippy.toml
#![deny(clippy::disallowed_types)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::todo)]
#![deny(clippy::unimplemented)]
#![deny(clippy::panic_in_result_fn)]

// Disallowed types (determinism)
use std::collections::{BTreeMap, BTreeSet};
// HashMap, HashSet, f32, f64, SystemTime, Instant are forbidden
```

### 13.5 Golden Fixtures

```rust
#[test]
fn golden_checkpoint_v0() {
    let checkpoint = Checkpoint {
        checkpoint_id: "cp_test_001".to_string(),
        session_id: SessionId::from_bytes([1u8; 16]),
        step_index: 42,
        total_actions: 100,
        replay_actions: vec![
            ReplayAction::ReadFile {
                path: "/test/file.txt".to_string(),
                expected_hash: "a3f7c2d8...".to_string(),
            }
        ],
        artifact_refs: vec![],
        handle_registry: vec![],
        determinism_context: DeterminismContext {
            seed: 12345,
            model_version: "claude-3.5-sonnet-20241022".to_string(),
            input_hash: "deadbeef...".to_string(),
            checkpoint_format_version: 0,
            worker_type: WorkerType::Python,
        },
        created_at: 1717098723000,
    };
    
    let serialized = rmp_serde::to_vec_named(&checkpoint).unwrap();
    let expected = include_bytes!("../../fixtures/checkpoint_v0.msgpack");
    
    assert_eq!(
        serialized, 
        expected,
        "Checkpoint serialization diverged from golden fixture.\n\
         This indicates a non-deterministic change in the serialization format."
    );
}
```

---

## 14. Phoenix Test Framework

### 14.1 Test Harness

```rust
// crates/phoenix-tests/src/harness.rs

pub struct PhoenixHarness {
    temp_dir: TempDir,
    db_path: PathBuf,
    vault_path: PathBuf,
}

impl PhoenixHarness {
    pub async fn new() -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("state.sqlite");
        let vault_path = temp_dir.path().join("vault");
        tokio::fs::create_dir(&vault_path).await.unwrap();
        
        Self { temp_dir, db_path, vault_path }
    }
    
    pub async fn create_session(&self, intent: &str) -> TestSession {
        let pool = SqlitePool::connect(&format!("sqlite:{}", self.db_path.display()))
            .await.unwrap();
        sqlx::query(CREATE_SCHEMA).execute(&pool).await.unwrap();
        
        let session_id = SessionId::new();
        let event = NexusEvent {
            event_id: generate_event_id(),
            event_type: EventType::IntentReceived {
                raw_input: intent.to_string(),
                source: "phoenix".to_string(),
            },
            session_id,
            trace_id: generate_trace_id(),
            parent_event_id: None,
            causal_vector: CausalVector::new(),
            payload: vec![],
            payload_hash: String::new(),
            event_timestamp: now_millis(),
            nonce: generate_nonce(),
            integrity_hash: String::new(),
        };
        
        TestSession {
            id: session_id,
            db: pool,
            vault: self.vault_path.clone(),
        }
    }
    
    pub async fn wait_for_checkpoint(
        &self,
        session: &TestSession,
        step_index: u64,
    ) {
        loop {
            let state = sqlx::query_as::<_, NexusState>(
                "SELECT * FROM sessions WHERE session_id = ?"
            )
            .bind(session.id.as_bytes())
            .fetch_one(&session.db)
            .await
            .unwrap();
            
            if state.checkpoint_seq >= step_index { break; }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    
    pub async fn kill_session(&self, session: &TestSession, signal: Signal) {
        let pid = self.find_worker_pid(session).await;
        nix::sys::signal::kill(pid, signal).unwrap();
        
        loop {
            match nix::sys::signal::kill(pid, None) {
                Err(nix::errno::Errno::ESRCH) => break,
                _ => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    }
    
    pub async fn resume_session(&self, session: &TestSession) -> Result<<RecoveredSession, PhoenixError> {
        let store = SqliteEventStore::new(&format!("sqlite:{}", self.db_path.display())).await?;
        let recovered = recover_session(&store, session.id).await?;
        Ok(recovered)
    }
}
```

### 14.2 Invariant Checks

```rust
// crates/phoenix-tests/src/invariants.rs

pub struct PhoenixInvariants;

impl PhoenixInvariants {
    pub async fn check_all(recovered: &RecoveredSession) -> Result<(), PhoenixError> {
        Self::check_event_log_integrity(recovered).await?;
        Self::check_causal_monotonicity(recovered).await?;
        Self::check_no_duplicate_llm(recovered).await?;
        Self::check_state_replay_consistency(recovered).await?;
        Self::check_budget_consistency(recovered).await?;
        Self::check_artifact_integrity(recovered).await?;
        Self::check_capability_non_forgeability(recovered).await?;
        Self::check_side_effect_idempotency(recovered).await?;
        Ok(())
    }
    
    /// I-1: State Authority — SQLite database passes PRAGMA integrity_check
    pub async fn i1_state_authority(db: &SqlitePool) -> Result<(), String> {
        let row: (String,) = sqlx::query_as("PRAGMA integrity_check")
            .fetch_one(db).await.map_err(|e| e.to_string())?;
        if row.0 != "ok" {
            return Err(format!("integrity_check failed: {}", row.0));
        }
        Ok(())
    }
    
    /// I-2: Checkpoint Identity — checkpoint ID and step_index survive restart
    pub async fn i2_checkpoint_identity(before: &Checkpoint, after: &Checkpoint) -> Result<(), String> {
        if before.checkpoint_id != after.checkpoint_id {
            return Err("checkpoint_id changed".to_string());
        }
        if before.step_index != after.step_index {
            return Err("step_index changed".to_string());
        }
        Ok(())
    }
    
    /// I-3: Replay Integrity — Event replay produces byte-identical state
    pub async fn i3_replay_integrity(events: &[NexusEvent], expected: &NexusState) -> Result<(), String> {
        let mut replayed = NexusState::new(expected.session_id, 0);
        let dag = BTreeMap::new();
        for event in events {
            replayed = transition(&replayed, event, &dag)
                .map_err(|e| format!("transition failed: {:?}", e))?;
        }
        if replayed.version != expected.version {
            return Err(format!("version mismatch: replayed={}, expected={}", replayed.version, expected.version));
        }
        Ok(())
    }
    
    /// I-4: Artifact Integrity — blake3 hashes of vault files remain valid
    pub async fn i4_artifact_integrity(artifacts: &[ArtifactRef]) -> Result<(), String> {
        for art in artifacts {
            let path = resolve_vault_path(&art.uri);
            let content = tokio::fs::read(&path).await
                .map_err(|e| format!("read failed for {}: {}", art.id, e))?;
            let actual = blake3::hash(&content);
            if actual.to_hex().to_string() != art.blake3 {
                return Err(format!("hash mismatch for {}: expected={}, actual={}", art.id, art.blake3, actual.to_hex()));
            }
        }
        Ok(())
    }
    
    /// I-5: Determinism Context — seed, model_version, input_hash preserved
    pub async fn i5_determinism_context(before: &DeterminismContext, after: &DeterminismContext) -> Result<(), String> {
        if before.seed != after.seed { return Err("seed changed".to_string()); }
        if before.model_version != after.model_version { return Err("model_version changed".to_string()); }
        if before.input_hash != after.input_hash { return Err("input_hash changed".to_string()); }
        Ok(())
    }
    
    /// I-6: Cost Integrity — No duplicated llm_calls or side_effects
    pub async fn i6_cost_integrity(db: &SqlitePool, session_id: SessionId) -> Result<(), String> {
        let llm_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(DISTINCT request_id) FROM llm_calls WHERE session_id = ?"
        ).bind(session_id.as_bytes()).fetch_one(db).await.map_err(|e| e.to_string())?;
        
        let llm_total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM llm_calls WHERE session_id = ?"
        ).bind(session_id.as_bytes()).fetch_one(db).await.map_err(|e| e.to_string())?;
        
        if llm_count != llm_total {
            return Err(format!("duplicate LLM calls detected: {} unique, {} total", llm_count, llm_total));
        }
        Ok(())
    }
    
    /// I-7: Resume Continuity — Execution resumes from step N+1, not N
    pub async fn i7_resume_continuity(before_seq: u64, after_seq: u64) -> Result<(), String> {
        if after_seq <= before_seq {
            return Err(format!("did not progress: before={}, after={}", before_seq, after_seq));
        }
        Ok(())
    }
    
    /// I-8: Eventual Consistency — All committed transitions reconstructable from event_log
    pub async fn i8_eventual_consistency(db: &SqlitePool, session_id: SessionId) -> Result<(), String> {
        let events: Vec<NexusEvent> = sqlx::query_as(
            "SELECT * FROM events WHERE session_id = ? ORDER BY event_timestamp"
        ).bind(session_id.as_bytes()).fetch_all(db).await.map_err(|e| e.to_string())?;
        
        let mut replayed = NexusState::new(session_id, 0);
        let dag = BTreeMap::new();
        for event in &events {
            replayed = transition(&replayed, event, &dag)
                .map_err(|e| format!("replay failed at {}: {:?}", event.event_id, e))?;
        }
        
        let stored: Option<NexusState> = sqlx::query_as(
            "SELECT * FROM sessions WHERE session_id = ?"
        ).bind(session_id.as_bytes()).fetch_optional(db).await.map_err(|e| e.to_string())?;
        
        if let Some(stored) = stored {
            if replayed.version != stored.version {
                return Err(format!("materialized view diverged: replayed_v={}, stored_v={}", replayed.version, stored.version));
            }
        }
        Ok(())
    }
}
```

### 14.3 Phoenix Test Suite

```rust
// crates/phoenix-tests/src/lib.rs

pub struct PhoenixSuite;

impl PhoenixSuite {
    pub async fn run_all() -> Result<<PhoenixReport, PhoenixError> {
        let mut report = PhoenixReport::default();
        
        report.tests.push(Self::test_kill9_at_intake().await?);
        report.tests.push(Self::test_kill9_at_planning().await?);
        report.tests.push(Self::test_kill9_at_executing().await?);
        report.tests.push(Self::test_kill9_at_checkpoint().await?);
        report.tests.push(Self::test_kill9_at_converging().await?);
        report.tests.push(Self::test_kill9_at_reflecting().await?);
        report.tests.push(Self::test_worker_crash().await?);
        report.tests.push(Self::test_llm_api_timeout().await?);
        report.tests.push(Self::test_side_effect_crash().await?);
        report.tests.push(Self::test_cross_session_resume().await?);
        
        Ok(report)
    }
    
    async fn test_kill9_at_executing() -> Result<<PhoenixTestResult, PhoenixError> {
        let harness = PhoenixHarness::new().await;
        let session = harness.create_session("refactor auth").await;
        
        // Drive to execution
        harness.run_to_status(&session, SessionStatus::Executing).await?;
        harness.wait_for_checkpoint(&session, 1).await?;
        
        // SIGKILL
        harness.kill_session(&session, Signal::SIGKILL).await?;
        
        // Resume
        let resumed = harness.resume_session(&session).await?;
        
        // Verify all invariants
        PhoenixInvariants::check_all(&resumed).await?;
        
        // Critical: LLM not re-called
        assert_eq!(resumed.llm_call_count, 1, "LLM must not be re-called");
        
        // Critical: execution continues from checkpoint
        assert!(resumed.state.checkpoint_seq >= 1);
        
        Ok(PhoenixTestResult {
            name: "kill9_at_executing",
            passed: true,
        })
    }
    
    async fn test_cross_session_resume() -> Result<<PhoenixTestResult, PhoenixError> {
        let harness = PhoenixHarness::new().await;
        
        // Session A: complete work
        let session_a = harness.create_session("research topic").await;
        harness.run_to_completion(&session_a).await?;
        
        // Export
        let export = harness.export_session(&session_a).await?;
        
        // Import as Session B
        let session_b = harness.import_session(&export).await?;
        
        // Verify memory inheritance
        assert!(!session_b.inherited_memories.is_empty());
        
        Ok(PhoenixTestResult {
            name: "cross_session_resume",
            passed: true,
        })
    }
}
```

---

## 15. Performance Engineering

### 15.1 Targets

| Metric | Target | Measurement |
|---|---|---|
| State transition | < 1ms | Single event, SQLite WAL |
| Event append | < 5ms | Single event, fsync=NORMAL |
| Batch event append | > 10,000/s | 100 events per transaction |
| Recovery | < 2s | 1000 events, SSD |
| Worker spawn | < 500ms | Local process, cold start |
| Worker checkpoint | < 100ms | Stdio round-trip |
| Memory per session | < 1MB | Excluding artifacts |
| DB growth | < 100MB/day | Typical dev usage |

### 15.2 Optimization Strategies

```rust
// Batch event insertion
pub async fn append_batch(
    &self,
    events: &[NexusEvent],
) -> Result<(), StoreError> {
    let _guard = self.write_lock.lock().await;
    let mut tx = self.pool.begin().await?;
    
    for event in events {
        sqlx::query("INSERT INTO events ...")
            .bind(...)
            .execute(&mut *tx)
            .await?;
    }
    
    // Single commit = single fsync
    tx.commit().await?;
    Ok(())
}

// Connection pool tuning
let pool = SqlitePoolOptions::new()
    .max_connections(1)  // Single writer
    .min_connections(1)
    .acquire_timeout(Duration::from_secs(5))
    .connect(database_url)
    .await?;

// WAL checkpoint tuning
sqlx::query("PRAGMA wal_autocheckpoint=1000").execute(&pool).await?;
```

---

## 16. Build & Deployment

### 16.1 Lite Mode (Zero Infrastructure)

```yaml
# docker-compose.lite.yml
version: '3.8'
services:
  nexus:
    image: nexus/lite:latest
    volumes:
      - ~/.nexus:/data
    environment:
      - NEXUS_MODE=lite
      - NEXUS_DB_PATH=/data/events.db
      - NEXUS_VAULT_PATH=/data/vault
```

### 16.2 Pro Mode (Docker)

```yaml
# docker-compose.pro.yml
version: '3.8'
services:
  nexus:
    image: nexus/pro:latest
    ports:
      - "3000:3000"
    environment:
      - NEXUS_MODE=pro
      - NEXUS_DB_URL=postgres://nexus:password@postgres:5432/nexus
      - NEXUS_REDIS_URL=redis://redis:6379
    depends_on:
      - postgres
      - redis
  
  postgres:
    image: postgres:16
    environment:
      - POSTGRES_USER=nexus
      - POSTGRES_PASSWORD=password
      - POSTGRES_DB=nexus
    volumes:
      - postgres_data:/var/lib/postgresql/data
  
  redis:
    image: redis:7-alpine
    volumes:
      - redis_data:/data

volumes:
  postgres_data:
  redis_data:
```

### 16.3 Enterprise Mode (Kubernetes)

```yaml
# k8s/enterprise/nexus-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nexus-kernel
spec:
  replicas: 3
  selector:
    matchLabels:
      app: nexus-kernel
  template:
    metadata:
      labels:
        app: nexus-kernel
    spec:
      containers:
      - name: kernel
        image: nexus/enterprise:latest
        env:
        - name: NEXUS_MODE
          value: enterprise
        - name: TEMPORAL_HOST
          value: temporal-frontend:7233
        - name: DB_URL
          valueFrom:
            secretKeyRef:
              name: nexus-db-secret
              key: url
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "2000m"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nexus-worker
spec:
  replicas: 10
  selector:
    matchLabels:
      app: nexus-worker
  template:
    metadata:
      labels:
        app: nexus-worker
    spec:
      containers:
      - name: python-worker
        image: nexus-runtime/python-worker:v1.0.0
        resources:
          limits:
            cpu: "500m"
            memory: "512Mi"
        securityContext:
          readOnlyRootFilesystem: true
          allowPrivilegeEscalation: false
          capabilities:
            drop:
            - ALL
```

### 16.4 Build Requirements

| Component | Version | Purpose |
|---|---|---|
| Rust | 1.78.0 | Core runtime |
| Cargo | 1.78.0 | Build system |
| SQLite | 3.45+ | Lite mode storage |
| Python | 3.11+ | Python Worker |
| Node.js | 20+ | Node.js Worker |

### 16.5 Build Commands

```bash
# Clone and bootstrap
git clone https://github.com/nexus-runtime/nexus.git
cd nexus

# Build core
cargo build --release

# Run tests
cargo test
cargo test phoenix_ -- --nocapture

# Run demo
./scripts/demo.sh

# Install CLI
cargo install --path crates/nexus-core

# Build Python Worker
cd workers/python-worker
pip install -e .

# Build Node.js Worker
cd workers/node-worker
npm install
```

---

## 17. Error Handling

### 17.1 Error Taxonomy

| Layer | Error Type | Recovery Strategy |
|---|---|---|
| State Machine | `TransitionError` | Log, reject event, alert |
| Event Store | `StoreError` | Retry with backoff, circuit break |
| Worker | JSON-RPC error | Kill worker, reschedule task |
| Side Effect | `EffectError` | Class-dependent (replay/compensate/query) |
| Security | `CapabilityError` | Reject action, audit log, alert |
| Recovery | `RecoveryError` | Human escalation if automatic fails |
| Network | Timeout | Retry with exponential backoff |

### 17.2 Error Propagation Rules

1. **State machine errors are fatal to the event** — invalid events are rejected, not applied
2. **Store errors are retryable** — SQLite busy/locked triggers exponential backoff
3. **Worker errors trigger task retry** — up to `RetryPolicy.max_attempts`
4. **Side-effect errors respect class** — Pure/Idempotent replay; Reversible compensate; Irreversible escalate
5. **Security errors are never retried** — capability denial is permanent for the token lifetime

---

## 18. Observability

### 18.1 Metrics

| Metric | Type | Labels |
|---|---|---|
| `nexus_events_appended_total` | Counter | `event_type`, `session_status` |
| `nexus_transitions_total` | Counter | `from_status`, `to_status` |
| `nexus_workers_spawned_total` | Counter | `worker_type` |
| `nexus_workers_failed_total` | Counter | `error_code` |
| `nexus_recovery_duration_ms` | Histogram | `events_replayed` |
| `nexus_checkpoint_size_bytes` | Histogram | `step_index` |
| `nexus_side_effects_committed_total` | Counter | `effect_class` |
| `nexus_llm_calls_total` | Counter | `model` |
| `nexus_llm_cost_cents_total` | Counter | `model`, `session_id` |
| `nexus_memory_graph_nodes` | Gauge | `session_id` |
| `nexus_entropy_score` | Gauge | — |

### 18.2 Structured Logging

```rust
tracing::info!(
    target = "nexus.runtime",
    event_id = %event.event_id,
    session_id = %session_id.to_hex(),
    event_type = %event.event_type.as_str(),
    causal_vector = %event.causal_vector.to_canonical(),
    version = state.version,
    "Event appended"
);
```

### 18.3 Tracing

- **Trace ID:** 16-byte hex, propagated across all events in a session
- **Span hierarchy:** session → task → worker → action → side_effect
- **Export:** OpenTelemetry OTLP to collector (optional in Lite mode)

---

## 19. Implementation Roadmap

| Phase | Duration | Deliverables | Gate |
|---|---|---|---|
| **0** | 2 weeks | Protocol spec, schema freeze, golden fixtures | 3 reviewers confirm consistency |
| **1** | 4 weeks | `nexus-core`, `nexus-event-store` (SQLite), `nexus-rpc`, `nexus-security`, `phoenix-tests`, Python Worker, CLI | `cargo test phoenix_` 100% pass; `demo.sh` outputs `RECOVERY SUCCESSFUL` |
| **2** | 4 weeks | PostgreSQL adapter, Docker scheduler, OpenClaw/Hermes adapters, export/import | Cross-tool session migration verified |
| **3** | 6 weeks | Temporal adapter, K8s scheduler, distributed causal bus, multi-agent coordination | 100 concurrent sessions, 99.9% recovery |
| **4** | 8 weeks | Python/Node.js/Rust SDKs, skill marketplace, web dashboard, documentation | 10 external contributors, 3 production deployments |

---

## 20. Repository Structure

```
nexus/
├── Cargo.toml
├── Cargo.lock
├── rust-toolchain.toml          # channel = "1.78.0"
├── LICENSE                      # MIT + Apache 2.0
├── README.md
│
├── crates/
│   ├── nexus-core/              # State machine + types + protocol
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── state_machine.rs # transition() pure function
│   │   │   ├── types.rs         # SessionId, TaskId, CausalVector, etc.
│   │   │   ├── event.rs         # NexusEvent, EventType
│   │   │   ├── checkpoint.rs    # Checkpoint, ReplayAction
│   │   │   ├── memory.rs        # MemoryGraph, MemoryNode, CausalEdge
│   │   │   ├── recovery.rs      # resume_session()
│   │   │   ├── effects.rs       # EffectGuard, SideEffectIntent
│   │   │   ├── entropy.rs       # EntropyController
│   │   │   └── protocol.rs      # Serialization spec (rmp-serde)
│   │   └── Cargo.toml
│   │
│   ├── nexus-event-store/       # Storage abstraction + implementations
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── sqlite.rs        # SQLite implementation
│   │   │   ├── postgres.rs      # PostgreSQL implementation
│   │   │   ├── temporal.rs      # Temporal adapter
│   │   │   └── schema.rs        # SQL schema
│   │   └── Cargo.toml
│   │
│   ├── nexus-scheduler/         # Worker scheduling
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── local.rs         # Local process scheduler
│   │   │   ├── docker.rs        # Docker container scheduler
│   │   │   └── k8s.rs           # Kubernetes scheduler
│   │   └── Cargo.toml
│   │
│   ├── nexus-rpc/               # JSON-RPC codec
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── codec.rs         # canonicalize_worker_payload
│   │   │   └── protocol.rs      # JSON-RPC 2.0 types
│   │   └── Cargo.toml
│   │
│   ├── nexus-security/          # Capability tokens + sandbox
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── capability.rs    # HMAC-SHA256 tokens
│   │   │   └── sandbox.rs       # Landlock/seccomp
│   │   └── Cargo.toml
│   │
│   └── phoenix-tests/           # Acceptance test framework
│       ├── src/
│       │   ├── lib.rs
│       │   ├── harness.rs       # PhoenixHarness
│       │   ├── invariants.rs    # 8 Phoenix invariants
│       │   └── fixtures.rs      # Golden test vectors
│       └── Cargo.toml
│
├── workers/
│   ├── python-worker/           # Python Worker template
│   │   ├── main.py
│   │   ├── requirements.txt
│   │   └── README.md
│   │
│   ├── node-worker/             # Node.js Worker template
│   │   ├── main.js
│   │   ├── package.json
│   │   └── README.md
│   │
│   └── rust-worker/             # Rust Worker template
│       ├── Cargo.toml
│       └── src/main.rs
│
├── adapters/
│   ├── openclaw/                # OpenClaw Gateway Adapter
│   │   ├── src/
│   │   └── Cargo.toml
│   │
│   └── hermes/                  # Hermes CLI Adapter
│       ├── src/
│       └── Cargo.toml
│
├── sdk/
│   ├── python/                  # Python SDK
│   │   ├── nexus/
│   │   │   ├── __init__.py
│   │   │   ├── runtime.py
│   │   │   ├── session.py
│   │   │   └── memory.py
│   │   ├── setup.py
│   │   └── README.md
│   │
│   ├── nodejs/                  # Node.js SDK
│   │   ├── src/
│   │   ├── package.json
│   │   └── README.md
│   │
│   └── rust/                    # Rust SDK (re-exports nexus-core)
│       ├── src/
│       └── Cargo.toml
│
├── docs/
│   ├── protocol/
│   │   ├── overview.md
│   │   ├── event-schema.md
│   │   ├── state-machine.md
│   │   ├── serialization.md
│   │   └── worker-protocol.md
│   │
│   ├── architecture.md          # This document
│   ├── phoenix.md               # Recovery guarantees
│   ├── security.md              # Capability system
│   ├── determinism.md           # Serialization guarantees
│   └── adr/                     # Architecture Decision Records
│       ├── ADR-001-deterministic-runtime.md
│       ├── ADR-002-temporal-substrate.md
│       ├── ADR-003-llm-events.md
│       ├── ADR-004-authority-boundary.md
│       └── ADR-005-governance-hot-cold.md
│
├── fixtures/                    # Golden checkpoint fixtures
│   ├── checkpoint_v0.msgpack
│   ├── transition_tests.json
│   └── causal_vectors.json
│
├── scripts/
│   ├── bootstrap.sh
│   ├── demo.sh                  # One-command demo
│   └── phoenix-runner.py        # CI test runner
│
└── policies/
    ├── rego/                    # OPA/Rego policies (Hot Path)
    │   ├── budget.rego
    │   ├── capabilities.rego
    │   └── rate_limit.rego
    │
    └── yaml/                    # YAML policies (Warm/Cold Path)
        ├── default.yaml
        └── strict.yaml
```

---

## 21. Appendices

### Appendix A: ADR Summaries

| ADR | Title | Status |
|---|---|---|
| **ADR-001** | Deterministic Runtime vs Probabilistic Cognition | Accepted |
| **ADR-002** | Temporal as Durable Execution Substrate (Enterprise Optional) | Accepted |
| **ADR-003** | LLM Output as Externalized Events | Accepted |
| **ADR-004** | Runtime Authority Boundary | Accepted |
| **ADR-005** | Governance Hot Path vs Cold Path | Accepted |
| **ADR-006** | SQLite as Default Event Store (Lite/Pro) | Accepted |
| **ADR-007** | JSON-RPC over stdio for Worker Communication | Accepted |
| **ADR-008** | BLAKE3 for Content Addressing | Accepted |

**ADR-001: Deterministic Runtime vs Probabilistic Cognition**
- LLM outputs are non-deterministic but must be treated as external side effects
- Runtime state transitions must be 100% deterministic
- Separation: LLM proposes, Runtime validates, Execution commits

**ADR-002: Temporal as Durable Execution Substrate (Enterprise Optional)**
- Temporal provides proven durable execution for enterprise deployments
- Lite/Pro modes use SQLite/PostgreSQL with custom replay logic
- Temporal is optional, not required

**ADR-003: LLM Output as Externalized Events**
- LLM calls are Activities with cached results
- Results stored in event log, never re-called during recovery
- Prompt hash + response hash for integrity verification

**ADR-004: Runtime Authority Boundary**
- Three-layer architecture: Cognition → Runtime → Execution
- LLM has read-only access to state
- Workers have no direct state mutation capability

**ADR-005: Governance Hot Path vs Cold Path**
- Hot path (< 1ms): budget, capability, rate limit checks
- Warm path (< 100ms): policy engine (Rego/WASM)
- Cold path (async): human approval, complex risk analysis

### Appendix B: Serialization Specification

| Property | Value |
|---|---|
| **Format** | MessagePack (`rmp-serde`) |
| **Mode** | StructMap (not ArrayMap) |
| **Integer Encoding** | BigEndian |
| **Float Encoding** | Prohibited (use `u64` cents for currency) |
| **Map Type** | `BTreeMap` (deterministic ordering) |
| **Set Type** | `BTreeSet` (deterministic ordering) |
| **DateTime** | `u64` milliseconds since Unix epoch |
| **UUID** | 16-byte array (not string) |

**Forbidden Types in Authority Structures:**
- `HashMap`, `HashSet` (non-deterministic ordering)
- `f32`, `f64` (precision issues, non-deterministic)
- `serde_json::Value` (schema-less, non-deterministic)
- `SystemTime`, `Instant` (system-dependent)

**Required Types:**
- `BTreeMap<K, V>` (deterministic ordering)
- `BTreeSet<T>` (deterministic ordering)
- `Vec<T>` (ordered sequence)
- `u64` (unsigned integer)
- `String` (UTF-8)

### Appendix C: Error Codes

| Code | Name | Description |
|---|---|---|
| -32700 | ParseError | Invalid JSON |
| -32600 | InvalidRequest | JSON-RPC request invalid |
| -32601 | MethodNotFound | Method does not exist |
| -32602 | InvalidParams | Invalid method parameters |
| -32603 | InternalError | Internal Worker error |
| -32001 | CapabilityDenied | Capability token insufficient |
| -32002 | BudgetExceeded | Session budget exhausted |
| -32003 | SandboxViolation | Worker attempted unauthorized action |
| -32004 | Timeout | Worker execution timeout |
| -32005 | CheckpointStale | Checkpoint step_index not monotonic |

### Appendix D: Event Type Catalog

| Type | Phase | Description | Payload Schema |
|---|---|---|---|
| `INTENT_RECEIVED` | Intake | User input captured | `{raw_input: string, source: string}` |
| `INTENT_PARSED` | Intake | Intent graph generated | `{intent_graph: IntentGraph}` |
| `PLAN_PROPOSED` | Planning | LLM proposes plan | `{plan: ExecutionPlan, model: string, tokens: {...}}` |
| `PLAN_COMMITTED` | Planning | Plan validated and committed | `{frontier: Frontier}` |
| `PLAN_REJECTED` | Planning | Plan validation failed | `{reason: string}` |
| `DEPENDENCIES_MET` | Execution | Precondition check passed | `{}` |
| `FRONTIER_VALIDATED` | Execution | Frontier validation passed | `{validation_result: ValidationResult}` |
| `WORKER_DISPATCHED` | Execution | Worker assigned | `{worker_id: string, task_id: TaskId}` |
| `WORKER_STARTED` | Execution | Worker process started | `{worker_id: string, task_id: TaskId, pid: u32}` |
| `WORKER_CHECKPOINT` | Execution | Progress snapshot | `{task_id: TaskId, step_index: u64, actions: [...]}` |
| `WORKER_COMPLETED` | Execution | Task finished | `{worker_id: string, result: WorkerResult}` |
| `WORKER_FAILED` | Execution | Task failed | `{worker_id: string, error: string, code: ErrorCode}` |
| `CONVERGE_STARTED` | Convergence | Multi-worker merge begins | `{task_ids: [TaskId]}` |
| `CONVERGE_COMPLETE` | Convergence | Merge finished | `{merged_result: WorkerResult}` |
| `REFLECTION_STARTED` | Reflection | Post-execution evaluation | `{checkpoint_seq: u64}` |
| `REFLECTION_COMPLETE` | Reflection | Evaluation complete | `{evaluation: Evaluation, memory_delta: [...]}` |
| `MEMORY_CONSOLIDATED` | Reflection | Memory merged | `{memory_ids: [string]}` |
| `SIDE_EFFECT_INTENT` | Side Effect | External action intent | `{effect: SideEffectIntent}` |
| `SIDE_EFFECT_COMMITTED` | Side Effect | Action executed | `{effect_id: string, result_hash: string}` |
| `SIDE_EFFECT_COMPENSATED` | Side Effect | Action undone | `{effect_id: string, compensation_result: string}` |
| `HUMAN_APPROVAL_REQUESTED` | Governance | Action blocked | `{action: Action, reason: string}` |
| `HUMAN_APPROVED` | Governance | Reviewer approved | `{approver: string}` |
| `HUMAN_REJECTED` | Governance | Reviewer rejected | `{rejecter: string, reason: string}` |
| `POLICY_DECISION` | Governance | Policy evaluated | `{policy_id: string, decision: PolicyDecision}` |
| `SESSION_SUSPENDED` | Lifecycle | Session paused | `{reason: string}` |
| `SESSION_RESUMED` | Lifecycle | Session resumed | `{from_checkpoint: u64, inherited_memories: [...]}` |
| `SESSION_MIGRATED` | Lifecycle | Session exported/imported | `{from: SessionId, to: SessionId, export_hash: string}` |
| `SESSION_ARCHIVED` | Lifecycle | Session completed | `{reason: string, final_status: SessionStatus}` |

### Appendix E: Glossary

| Term | Definition |
|---|---|
| **ActionIntent** | Proposed action generated by LLM, pending Runtime validation |
| **Artifact** | Immutable content-addressed output of task execution |
| **Capability Token** | HMAC-signed permission grant for Worker actions |
| **Causal Vector** | Vector clock tracking happens-before relationships |
| **Checkpoint** | Replayable execution progress snapshot (not memory dump) |
| **Effect Class** | Categorization of side effects: Pure, Idempotent, Reversible, Irreversible |
| **Entropy** | Quantitative measure of runtime instability |
| **Event Log** | Append-only immutable record; source of truth |
| **Frontier** | Bounded DAG fragment currently being executed |
| **Materialized View** | Query-optimized cache derived from event log |
| **Phoenix** | Acceptance test framework for recovery verification |
| **Side-Effect Guard** | Proxy layer controlling all external actions |
| **Worker** | Stateless, isolated execution unit |

### Appendix F: Related Specifications

- Nexus Protocol v1.0 (`docs/protocol/overview.md`)
- Worker RPC Specification (`docs/rpc.md`)
- Security Model (`docs/security.md`)
- Determinism Rules (`docs/determinism.md`)
- Phoenix Recovery Guarantees (`docs/phoenix.md`)

---

> **This specification is frozen. Changes require ADR and Technical Steering Committee approval.**
>
> **Nexus Runtime v1.0 — Comprehensive Technical Specification**
> **Implementation-ready reference for engineering teams.**