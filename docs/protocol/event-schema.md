# Event Schema

## Structure

Every event has the following envelope:

| Field | Type | Description |
|-------|------|-------------|
| `event_id` | `String` | Unique identifier |
| `event_type` | `EventType` | Tagged union discriminator |
| `session_id` | `SessionId` | 16-byte session UUID |
| `trace_id` | `[u8; 16]` | Trace propagation ID |
| `parent_event_id` | `Option<String>` | Causal parent |
| `causal_vector` | `CausalVector` | Vector clock at creation |
| `payload` | `Vec<u8>` | MessagePack-encoded payload |
| `payload_hash` | `String` | blake3 hash of payload |
| `event_timestamp` | `u64` | Unix millis |
| `nonce` | `String` | Uniqueness guarantee |
| `integrity_hash` | `String` | blake3 of all preceding fields |

## Event Type Catalog

See [Appendix D of the architecture spec](../../td.md#appendix-d-event-type-catalog) for the full list of 28 event types across Intake, Planning, Execution, Convergence, Reflection, Side Effect, Governance, and Lifecycle phases.
