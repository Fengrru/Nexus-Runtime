# Nexus Protocol v1.0 — State Machine Specification

## Overview

The Nexus State Machine is a **pure-function deterministic automaton** that is the sole authority for all state transitions. No code outside `transition()` is permitted to mutate session state.

## Design Invariants

1. `transition()` is a pure function — no IO, no clock, no random
2. All state mutations flow through `transition()` — no direct database writes
3. Optimistic locking on `version` field prevents concurrent mutation races
4. Illegal transitions return `TransitionError::IllegalTransition` — never panic
5. Byte-for-byte reproducible across independent processes (golden fixtures)

## State Transition Diagram

```
CREATED ──[IntentReceived]──→ INTAKE ──[IntentParsed]──→ PLANNING
                                                                │
                              ┌─[PlanCommitted]─────────────────┤
                              │                                 │
                              ↓                                 ↓
                          PLANNED ──[DependenciesMet]──→  FAILED
                              │
                    ┌─────────┼─────────┐
                    │         │         │
              EXECUTING  CONVERGING   BLOCKED
                    │         │         │
              ┌─────┤    REFLECTING   ──┤
              │     │         │
        CHECKPOINT  │    COMPLETED
              │     │
              └─────┘
```

## Function Signature

```rust
pub fn transition(
    current: &NexusState,
    event: &NexusEvent,
    dag: &BTreeMap<TaskId, TaskNode>,
) -> Result<NexusState, TransitionError>
```

## States (14)

| State | Description |
|-------|-------------|
| `Created` | Initial state after session creation |
| `Intake` | User intent received, awaiting parsing |
| `Planning` | Intent parsed, LLM proposes execution plan |
| `Planned` | Plan validated and committed |
| `Executing` | Workers actively executing tasks |
| `Checkpointing` | Progress snapshot in progress |
| `Blocked` | Awaiting human approval |
| `Converging` | Multiple worker results being merged |
| `Reflecting` | Post-execution evaluation |
| `Completed` | Session finished successfully |
| `Failed` | Session terminated with error |
| `Archived` | Session frozen for audit |

## Event Types (24)

See `docs/protocol/event-schema.md` for the full event type catalog.
