# Nexus Protocol v1.0 — Overview

## Core Concepts

### Event Sourcing
The event log is the **source of truth**. All state mutations are recorded as immutable events. Session state is a **materialized view** derived from replaying events through the state machine.

### Deterministic State Machine
`transition()` is a pure function — no IO, no clock, no random, no async. Given the same `(state, event, dag)`, it always produces the same output. Verified by golden fixtures.

### Causal Consistency
Vector clocks (`CausalVector`) track happens-before relationships across sessions. Merging is commutative and idempotent.

### Stateless Workers
Workers communicate via JSON-RPC 2.0 over stdio (NDJSON). No network, no persistent state, no direct LLM access. All side effects are proxied through the Kernel.

## Protocol Files

- [State Machine](state-machine.md) — Full state transition diagram and `transition()` spec
- [Event Schema](event-schema.md) — All event types with payload schemas
- [Serialization](serialization.md) — Deterministic MessagePack rules and forbidden types
- [Worker Protocol](worker-protocol.md) — JSON-RPC methods, message formats, error codes
