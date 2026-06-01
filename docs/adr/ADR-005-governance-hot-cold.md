# ADR-005: Governance Hot Path vs Cold Path

**Status:** Accepted  
**Date:** 2026-05-22  
**Deciders:** Architecture Team

## Context

Policy enforcement must not become a bottleneck. Budget checks, capability validation, and rate limiting must execute in < 1ms (hot path). Complex risk analysis and human approval can execute asynchronously (cold path).

## Decision

**Three-tier policy evaluation:**

| Tier | Latency | Mechanism | Examples |
|------|---------|-----------|----------|
| **Hot Path** | < 1ms | Inline Rust checks | Budget, capability, rate limit |
| **Warm Path** | < 100ms | Policy engine (Rego/WASM) | Custom policies, compliance rules |
| **Cold Path** | Async | Human approval queue | Irreversible effects, budget override, security escalation |

- Hot path checks are hardcoded in the Kernel — no external policy engine dependency
- Warm path uses OPA/Rego policies loaded at runtime
- Cold path triggers `HUMAN_APPROVAL_REQUESTED` events, blocking the session until approved

## Consequences

- Session throughput is not bottlenecked by policy evaluation
- Hot path failures are immediate (budget exceeded, capability denied)
- Cold path provides safety valve for high-risk operations
