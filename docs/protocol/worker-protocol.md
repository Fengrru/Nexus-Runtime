# Worker Protocol

## Transport

JSON-RPC 2.0 over stdio with Newline-Delimited JSON (NDJSON) framing.

- **Encoding:** UTF-8, no BOM
- **Delimiter:** `\n` (0x0A)
- **Max message size:** 16 MB
- **Request timeout:** 30s

## Methods

### Core → Worker

| Method | Description |
|--------|-------------|
| `execute` | Execute a task with intent, capabilities, and inputs |
| `cancel` | Cancel a running task |

### Worker → Core (Notifications)

| Method | Description |
|--------|-------------|
| `checkpoint` | Progress snapshot with actions and artifacts |
| `progress` | Percentage and step description |

## Error Codes

| Code | Name |
|------|------|
| -32700 | ParseError |
| -32600 | InvalidRequest |
| -32601 | MethodNotFound |
| -32602 | InvalidParams |
| -32603 | InternalError |
| -32001 | CapabilityDenied |
| -32002 | BudgetExceeded |

See [Section 6 of the architecture spec](../../td.md#6-worker-protocol) for full message schemas.
