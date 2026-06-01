# ADR-003: LLM Output as Externalized Events

**Status:** Accepted  
**Date:** 2026-05-22  
**Deciders:** Architecture Team

## Context

LLM calls are expensive, non-deterministic, and must never be duplicated during crash recovery. The runtime needs a mechanism to cache LLM responses and treat them as immutable events.

## Decision

**LLM calls are Activities with cached results.** Results are stored in the event log as `PLAN_PROPOSED` events with prompt hash, response hash, and token counts.

- The `LlmProxy` intercepts all LLM calls from Workers (Workers have no direct API access)
- Each LLM response is cached by prompt hash; recovery replays cached results, never re-calls the API
- `llm_calls` table provides an immutable audit trail for cost governance
- ID idempotency: same prompt hash → same cached response, regardless of session

## Consequences

- Zero duplicate LLM calls during crash recovery (Phoenix invariant I-6)
- Cost governance: hard budget ceilings enforced by the proxy before API calls
- Audit trail: every LLM call is permanently recorded with cost, tokens, and model
