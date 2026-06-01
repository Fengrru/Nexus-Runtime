# ADR-004: Runtime Authority Boundary

**Status:** Accepted  
**Date:** 2026-05-22  
**Deciders:** Architecture Team

## Context

In agent systems, the boundary between "thinking" (LLM cognition) and "doing" (execution) is often blurred, leading to security vulnerabilities, state corruption, and non-deterministic recovery.

## Decision

**Three-layer authority boundary: LLM Proposes → Runtime Validates → Execution Commits**

- **LLM (Cognition layer):** Read-only access to state; proposes actions but cannot execute them
- **Runtime (Validation layer):** Validates proposals against capabilities, budget, and policy; owns causal ordering
- **Worker (Execution layer):** Stateless, capability-constrained; executes only validated actions

Workers communicate via JSON-RPC over stdio only — no network, no persistent state, no direct LLM access. All external calls (LLM APIs, tools, MCP servers) are routed through the Kernel proxy.

## Consequences

- Workers execute with least privilege (capability tokens, time-bounded)
- No secret material accessible to Workers
- All side effects go through two-phase intent protocol
- Kernel owns causality — Workers cannot fabricate or reorder events
