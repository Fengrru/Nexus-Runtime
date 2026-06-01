# ADR-001: Deterministic Runtime vs Probabilistic Cognition

**Status:** Accepted  
**Date:** 2026-05-22  
**Deciders:** Architecture Team

## Context

LLM outputs are inherently non-deterministic — the same prompt can produce different responses. However, the runtime infrastructure must be 100% deterministic to guarantee crash recovery, state replay, and cross-process consistency.

## Decision

**Separation of concerns:** LLM proposes, Runtime validates, Execution commits.

- LLM outputs are treated as **externalized events** — captured in the event log as side effects
- The runtime state machine (`transition()`) is a **pure function** — no IO, no async, no clock, no random
- State transitions are **byte-identical across independent processes** (verified by golden fixtures)

## Consequences

- LLM calls are cached in the event log; recovery replays cached results, never re-calls APIs
- The `SessionArchived` event type captures LLM outputs as immutable audit records
- Deterministic serialization (BTreeMap, u64, rmp-serde StructMap) enforced at CI level
