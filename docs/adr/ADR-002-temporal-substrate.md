# ADR-002: Temporal as Durable Execution Substrate (Enterprise Optional)

**Status:** Accepted  
**Date:** 2026-05-22  
**Deciders:** Architecture Team

## Context

Nexus Runtime requires durable execution guarantees — crash recovery, event sourcing, and deterministic replay. Temporal.io provides proven workflow recovery at enterprise scale, but introduces infrastructure dependency.

## Decision

**Temporal is optional and Enterprise-only.** The default implementation uses SQLite (Lite mode) and PostgreSQL (Pro mode) with custom replay logic.

- All three deployment modes share identical protocol semantics and state machine behavior
- The `EventStore` trait abstracts storage; Temporal is one implementation behind that trait
- Lite/Pro modes use `RecoveryManager` with custom event replay
- Enterprise mode adds Temporal for distributed workflow execution, signal handling, and query support

## Consequences

- No Temporal dependency required for Lite/Pro development
- Enterprise deployments can leverage Temporal's proven durability without protocol changes
- The `TemporalEventStore` delegates to a local store when Temporal is unavailable
