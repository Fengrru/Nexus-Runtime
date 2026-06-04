# Worker Development Guide

## Overview

Workers are stateless, isolated execution units that communicate with the Kernel via JSON-RPC 2.0 over stdio.

Workers have:
- No network access
- No persistent state
- No direct LLM access (proxied through Kernel)
- Only the capabilities granted by the Kernel

## Creating a Worker

### Python

```python
# python-worker/main.py
from nexus_worker import WorkerProtocol

class MyWorker(WorkerProtocol):
    def execute_intent(self, intent, inputs):
        # Your logic here
        pass

if __name__ == "__main__":
    MyWorker().run()
```

### Node.js

```javascript
// node-worker/main.js
const { WorkerProtocol } = require('@nexus/worker');

class MyWorker extends WorkerProtocol {
    handleExecute(msg) {
        // Your logic here
    }
}

new MyWorker().run();
```

### Rust

```rust
// workers/rust-worker/src/main.rs
use nexus_rpc::*;

struct MyWorker {
    // implementation
}
```

## Testing

Use `phoenix-tests` to verify worker crash recovery:

```bash
cargo test -p phoenix-tests -- --nocapture
```
