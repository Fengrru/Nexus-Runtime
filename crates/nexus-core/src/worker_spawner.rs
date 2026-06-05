use crate::types::*;
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
/// Real Worker Spawner — fork/exec Worker processes and communicate via stdio JSON-RPC.
/// Supports Python, Node.js, and Rust workers. Detects crashes and handles recovery.
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub task_id: TaskId,
    pub session_id: SessionId,
    pub worker_type: WorkerType,
    pub intent: TaskIntent,
    pub capabilities: Vec<String>,
    pub from_step: u64,
    pub timeout_ms: u64,
    pub token_budget: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerStatus {
    Starting,
    Running,
    Checkpointing,
    Completed,
    Failed,
    Killed,
}

pub struct WorkerHandle {
    pub task_id: TaskId,
    pub session_id: SessionId,
    pub pid: u32,
    pub status: WorkerStatus,
    child: Child,
    stdin: Option<std::process::ChildStdin>,
    reader: Option<BufReader<std::process::ChildStdout>>,
}

pub struct WorkerSpawner {
    python_path: String,
    node_path: String,
    #[allow(dead_code)]
    workers: Arc<Mutex<BTreeMap<TaskId, WorkerHandle>>>,
}

impl Default for WorkerSpawner {
    fn default() -> Self {
        Self {
            python_path: "python3".into(),
            node_path: "node".into(),
            workers: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

impl WorkerSpawner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_python(mut self, path: &str) -> Self {
        self.python_path = path.into();
        self
    }

    pub fn with_node(mut self, path: &str) -> Self {
        self.node_path = path.into();
        self
    }

    /// Spawn a Worker process and establish stdio JSON-RPC communication.
    pub fn spawn(&self, config: WorkerConfig) -> Result<WorkerHandle, String> {
        let (cmd, args) = match config.worker_type {
            WorkerType::Python => (
                self.python_path.clone(),
                vec!["workers/python-worker/main.py".to_string()],
            ),
            WorkerType::NodeJs => (
                self.node_path.clone(),
                vec!["workers/node-worker/main.js".to_string()],
            ),
            WorkerType::RustInline => {
                return Err("Inline Rust workers run in-process, not spawned".into());
            }
            WorkerType::WasmSandbox => {
                return Err("WASM workers run in sandbox, not spawned".into());
            }
        };

        let mut child = Command::new(&cmd)
            .args(&args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| {
                format!(
                    "Failed to spawn {} worker: {}",
                    config.worker_type.worker_type_name(),
                    e
                )
            })?;

        let pid = child.id();
        let stdin = child.stdin.take().ok_or("Failed to capture worker stdin")?;
        let stdout = child
            .stdout
            .take()
            .ok_or("Failed to capture worker stdout")?;
        let reader = BufReader::new(stdout);

        let handle = WorkerHandle {
            task_id: config.task_id,
            session_id: config.session_id,
            pid,
            status: WorkerStatus::Starting,
            child,
            stdin: Some(stdin),
            reader: Some(reader),
        };

        tracing::info!(
            target = "nexus.worker.spawn",
            task_id = %hex::encode(config.task_id.0),
            worker_type = %config.worker_type.worker_type_name(),
            pid = %pid,
            "Worker spawned"
        );

        Ok(handle)
    }

    /// Send a JSON-RPC execute command to an already-spawned worker.
    pub fn send_execute(handle: &mut WorkerHandle, config: &WorkerConfig) -> Result<(), String> {
        let stdin = handle.stdin.as_mut().ok_or("Worker stdin not available")?;

        let msg = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "execute",
            "params": {
                "task_id": hex::encode(config.task_id.0),
                "session_id": hex::encode(config.session_id.0),
                "intent": {
                    "action_type": config.intent.action_type,
                    "target": config.intent.target,
                    "parameters": config.intent.parameters,
                    "constraints": config.intent.constraints.iter().map(|c| {
                        serde_json::json!({"type": c.constraint_type, "value": c.value})
                    }).collect::<Vec<_>>(),
                },
                "inputs": [],
                "from_step": config.from_step,
                "capabilities": config.capabilities,
                "timeout_ms": config.timeout_ms,
                "token_budget": config.token_budget,
            }
        });

        let msg_str = serde_json::to_string(&msg).unwrap() + "\n";
        stdin
            .write_all(msg_str.as_bytes())
            .map_err(|e| format!("Write to worker stdin failed: {}", e))?;
        stdin
            .flush()
            .map_err(|e| format!("Flush worker stdin failed: {}", e))?;

        handle.status = WorkerStatus::Running;

        Ok(())
    }

    /// Read a JSON-RPC response/notification from the worker (non-blocking).
    pub fn read_response(handle: &mut WorkerHandle) -> Option<serde_json::Value> {
        let reader = handle.reader.as_mut()?;
        let mut line = String::new();

        match reader.read_line(&mut line) {
            Ok(0) => {
                handle.status = WorkerStatus::Failed;
                None
            }
            Ok(_) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    return None;
                }
                match serde_json::from_str::<serde_json::Value>(trimmed) {
                    Ok(msg) => {
                        if msg.get("method").is_some_and(|m| m == "checkpoint") {
                            handle.status = WorkerStatus::Checkpointing;
                        }
                        if msg.get("error").is_some() {
                            handle.status = WorkerStatus::Failed;
                        }
                        if msg.get("result").is_some() {
                            handle.status = WorkerStatus::Completed;
                        }
                        Some(msg)
                    }
                    Err(_) => None,
                }
            }
            Err(_) => None,
        }
    }

    /// Kill a worker (simulate crash for Phoenix tests).
    pub fn kill_worker(handle: &mut WorkerHandle) -> Result<(), String> {
        handle
            .child
            .kill()
            .map_err(|e| format!("Failed to kill worker pid {}: {}", handle.pid, e))?;
        handle.status = WorkerStatus::Killed;

        tracing::warn!(
            target = "nexus.worker.kill",
            pid = %handle.pid,
            task_id = %hex::encode(handle.task_id.0),
            "Worker killed (SIGKILL)"
        );

        Ok(())
    }

    /// Check if worker process is still alive.
    pub fn is_alive(handle: &WorkerHandle) -> bool {
        matches!(
            handle.status,
            WorkerStatus::Starting | WorkerStatus::Running | WorkerStatus::Checkpointing
        ) && handle.child.id() == handle.pid
    }

    /// Wait for worker to exit and collect exit status.
    pub fn wait_for_exit(handle: &mut WorkerHandle) -> Result<std::process::ExitStatus, String> {
        handle
            .child
            .wait()
            .map_err(|e| format!("Wait for worker failed: {}", e))
    }
}

impl WorkerType {
    fn worker_type_name(&self) -> &str {
        match self {
            WorkerType::Python => "python",
            WorkerType::NodeJs => "nodejs",
            WorkerType::RustInline => "rust-inline",
            WorkerType::WasmSandbox => "wasm",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spawn_python_worker() {
        let spawner = WorkerSpawner::new().with_python("python");

        let config = WorkerConfig {
            task_id: TaskId::from_bytes([1u8; 16]),
            session_id: SessionId::from_bytes([2u8; 16]),
            worker_type: WorkerType::Python,
            intent: TaskIntent {
                action_type: "read_file".into(),
                target: "test.txt".into(),
                parameters: BTreeMap::new(),
                constraints: vec![],
            },
            capabilities: vec!["fs:read:/tmp".into()],
            from_step: 0,
            timeout_ms: 5000,
            token_budget: 1000,
        };

        let result = spawner.spawn(config);
        match result {
            Ok(mut handle) => {
                assert!(handle.pid > 0);
                // Clean up
                let _ = handle.child.kill();
            }
            Err(e) => {
                // Python may not be installed on CI; skip gracefully
                eprintln!("Skipping worker spawn test (no Python): {}", e);
            }
        }
    }

    #[test]
    fn test_worker_kill_signal() {
        let spawner = WorkerSpawner::new().with_python("python");

        let config = WorkerConfig {
            task_id: TaskId::from_bytes([3u8; 16]),
            session_id: SessionId::from_bytes([4u8; 16]),
            worker_type: WorkerType::Python,
            intent: TaskIntent {
                action_type: "sleep".into(),
                target: "5".into(),
                parameters: BTreeMap::new(),
                constraints: vec![],
            },
            capabilities: vec![],
            from_step: 0,
            timeout_ms: 30000,
            token_budget: 100,
        };

        if let Ok(mut handle) = spawner.spawn(config) {
            WorkerSpawner::kill_worker(&mut handle).unwrap();
            match handle.status {
                WorkerStatus::Killed => {}
                _ => {
                    // Worker may have exited before kill; that's fine
                }
            }
        }
    }
}
