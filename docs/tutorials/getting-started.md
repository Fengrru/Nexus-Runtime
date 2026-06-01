# Nexus Runtime — Getting Started Tutorial

## Installation

### Rust (Core + CLI)
```bash
cargo install --path crates/nexus-cli
```

### Python SDK
```bash
pip install -e sdk/python
```

### Node.js SDK
```bash
cd sdk/nodejs && npm install
```

## Quick Start

### 1. Create a Session
```bash
nexus run "refactor authentication to JWT" --budget 5.00
```

### 2. Check Status
```bash
nexus status <session-id>
```

### 3. Simulate Crash & Recover
```bash
# Session running... then kill -9 the process
nexus resume <session-id>
# Execution continues from last checkpoint without LLM re-calls
```

### 4. Export for Cross-Tool Migration
```bash
nexus export <session-id> --output session.nexus
nexus import session.nexus
```

## Python Example
```python
from nexus import Runtime

with Runtime(mode="lite") as rt:
    session = rt.create_session("write tests for auth module", budget_usd=3.00)
    session.run()
    print(f"Session {session.id} status: {session.status.value}")

    # Survive crashes automatically
    resumed = rt.resume_session(session.id)
    print(f"Resumed at checkpoint {resumed.checkpoint_seq}")
```

## Node.js Example
```javascript
const { Runtime } = require('@nexus/runtime');

const rt = new Runtime({ mode: 'lite' });
const session = rt.createSession('build a REST API');
session.run();

console.log(session.toJSON());
```

## Phoenix Recovery Guarantees

Nexus survives `kill -9` at ANY execution step:
- **No LLM re-calls** — cached results in event log
- **No side-effect duplication** — two-phase intent protocol
- **No state corruption** — event-sourced replay
- **No causal drift** — vector clock monotonicity

## Next Steps
- [Protocol Specification](protocol/overview.md)
- [Worker Development Guide](tutorials/worker-development.md)
- [API Reference](api/)
