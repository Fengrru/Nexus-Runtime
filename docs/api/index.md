# Nexus Runtime API Reference

## Rust Crates

| Crate | Description | Key Types |
|-------|-------------|-----------|
| `nexus-core` | State machine, event system, types, recovery | `NexusState`, `NexusEvent`, `transition()`, `RecoveryManager` |
| `nexus-event-store` | `EventStore` trait + SQLite/PostgreSQL impls | `EventStore`, `SqliteEventStore`, `PostgresEventStore` |
| `nexus-rpc` | JSON-RPC 2.0 codec for Worker communication | `JsonRpcRequest`, `JsonRpcResponse`, `canonicalize_worker_payload` |
| `nexus-security` | Capability tokens (HMAC-SHA256), sandboxing | `CapabilityToken`, `CapabilityScope`, `SandboxTier` |
| `nexus-scheduler` | Topological + capability-aware task scheduling | `Scheduler`, `LocalScheduler`, `DockerScheduler`, `K8sScheduler` |
| `phoenix-tests` | 8-invariant crash recovery test framework | `PhoenixHarness`, `PhoenixInvariants` |

## Python SDK

```python
from nexus import Runtime

rt = Runtime(mode="lite")
session = rt.create_session("intent text", budget_usd=5.0)
session.run()
```

Key classes: `Runtime`, `Session`, `Memory`, `Budget`, `MemoryGraph`

## Node.js SDK

```javascript
const { Runtime } = require('@nexus/runtime');
const rt = new Runtime({ mode: 'lite' });
const session = rt.createSession('intent text');
```

Key classes: `Runtime`, `Session`, `Memory`, `MemoryGraph`, `Budget`
