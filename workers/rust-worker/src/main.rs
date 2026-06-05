/// Rust Worker for Nexus Runtime
/// JSON-RPC 2.0 over stdio (NDJSON framing)
///
/// Usage:
///     cargo run -p rust-worker
///
/// Protocol:
///     - Reads JSON-RPC messages from stdin (newline-delimited)
///     - Writes JSON-RPC responses/notifications to stdout
///     - No network access, no persistent state
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::process::Command;
use std::time::Instant;

use nexus_rpc::*;
use serde_json::{json, Value};

struct WorkerProtocol {
    task_id: Option<String>,
    session_id: Option<String>,
    capabilities: Vec<String>,
    step_index: u64,
}

impl WorkerProtocol {
    fn new() -> Self {
        Self {
            task_id: None,
            session_id: None,
            capabilities: Vec::new(),
            step_index: 0,
        }
    }

    fn log(&self, message: &str) {
        eprintln!("[Nexus Worker] {}", message);
    }

    fn send_message(&self, message: &Value) {
        let mut stdout = std::io::stdout();
        let line = serde_json::to_string(message).unwrap_or_default();
        let _ = writeln!(stdout, "{}", line);
        let _ = stdout.flush();
    }

    fn send_checkpoint(&self, step: u64, actions: &[Value], progress_percent: u32) {
        let notif = JsonRpcNotification {
            jsonrpc: "2.0".into(),
            method: "checkpoint".into(),
            params: Some(json!({
                "task_id": self.task_id,
                "step_index": step,
                "actions": actions,
                "progress_percent": progress_percent,
            })),
        };
        self.send_message(&json!(notif));
        self.log(&format!(
            "Checkpoint at step {} ({}%)",
            step, progress_percent
        ));
    }

    #[allow(dead_code)]
    fn send_progress(&self, percent: u32, current_step: &str, sub_steps: &[Value]) {
        let notif = JsonRpcNotification {
            jsonrpc: "2.0".into(),
            method: "progress".into(),
            params: Some(json!({
                "task_id": self.task_id,
                "percent": percent,
                "current_step": current_step,
                "sub_steps": sub_steps,
            })),
        };
        self.send_message(&json!(notif));
    }

    fn send_result(&self, request_id: &Option<Value>, result: ResultParams) {
        let resp = make_success_response(request_id.clone(), json!(result));
        self.send_message(&json!(resp));
    }

    fn send_error(&self, request_id: &Option<Value>, code: i32, message: &str) {
        let resp = make_error_response(request_id.clone(), code, message);
        self.send_message(&json!(resp));
    }

    fn create_artifact(&self, kind: &str, path: &str, content: &[u8]) -> ArtifactPayload {
        let hash = blake3::hash(content);
        let hash_hex = hash.to_hex();
        let short_hash = &hash_hex[..16];

        ArtifactPayload {
            id: format!("art_{}", short_hash),
            uri: format!("vault://artifacts/{}", short_hash),
            blake3: hash_hex.to_string(),
            size_bytes: content.len() as u64,
            kind: Some(kind.into()),
            metadata: Some({
                let mut m = BTreeMap::new();
                m.insert("path".into(), json!(path));
                m.insert("encoding".into(), json!("utf-8"));
                m
            }),
        }
    }

    fn handle_execute(&mut self, msg: &JsonRpcRequest) {
        let params: ExecuteParams = match msg.params.as_ref() {
            Some(p) => match serde_json::from_value(p.clone()) {
                Ok(params) => params,
                Err(e) => {
                    self.send_error(&msg.id, -32602, &format!("Invalid params: {}", e));
                    return;
                }
            },
            None => {
                self.send_error(&msg.id, -32602, "Missing params");
                return;
            }
        };

        self.task_id = Some(params.task_id.clone());
        self.session_id = Some(params.session_id.clone());
        self.capabilities = params.capabilities.clone();
        self.step_index = params.from_step;

        self.log(&format!(
            "Execute task: {} intent: {} -> {}",
            params.task_id, params.intent.action_type, params.intent.target
        ));
        self.log(&format!("Capabilities: {:?}", self.capabilities));

        let start = Instant::now();

        let result = self.execute_intent(&params.intent);
        let duration_ms = start.elapsed().as_millis() as u64;

        match result {
            Ok(artifacts) => {
                self.send_result(
                    &msg.id,
                    ResultParams {
                        status: "completed".into(),
                        artifacts,
                        metrics: MetricsPayload {
                            duration_ms,
                            tokens_consumed: 0,
                            cost_cents: 0,
                        },
                    },
                );
            }
            Err(e) => {
                self.send_error(&msg.id, -32603, &e);
            }
        }
    }

    fn execute_intent(
        &mut self,
        intent: &TaskIntentPayload,
    ) -> Result<Vec<ArtifactPayload>, String> {
        let mut artifacts: Vec<ArtifactPayload> = Vec::new();
        let action_type = intent.action_type.as_str();
        let target = intent.target.as_str();

        match action_type {
            "read_file" => {
                let content = std::fs::read(target).map_err(|e| format!("Read failed: {}", e))?;
                self.step_index += 1;
                self.send_checkpoint(
                    self.step_index,
                    &[json!({"type": "read_file", "path": target})],
                    50,
                );
                artifacts.push(self.create_artifact("file", target, &content));
            }

            "write_file" => {
                let content = intent
                    .parameters
                    .get("content")
                    .cloned()
                    .unwrap_or_default();
                std::fs::write(target, &content).map_err(|e| format!("Write failed: {}", e))?;
                self.step_index += 1;
                self.send_checkpoint(
                    self.step_index,
                    &[json!({"type": "write_file", "path": target})],
                    50,
                );
                artifacts.push(self.create_artifact("file", target, content.as_bytes()));
            }

            "grep" => {
                let pattern = intent
                    .parameters
                    .get("pattern")
                    .cloned()
                    .unwrap_or_default();
                let content =
                    std::fs::read_to_string(target).map_err(|e| format!("Read failed: {}", e))?;
                let matches: Vec<&str> = content
                    .lines()
                    .filter(|line| line.contains(&pattern))
                    .collect();
                let result_text = matches.join("\n");
                self.step_index += 1;
                self.send_checkpoint(
                    self.step_index,
                    &[json!({"type": "grep", "path": target})],
                    50,
                );
                artifacts.push(self.create_artifact("text", target, result_text.as_bytes()));
            }

            "run_command" => {
                let cmd_str = intent
                    .parameters
                    .get("command")
                    .cloned()
                    .unwrap_or_else(|| target.to_string());

                let output = if cfg!(windows) {
                    Command::new("cmd")
                        .args(["/C", &cmd_str])
                        .output()
                        .map_err(|e| format!("Command failed: {}", e))?
                } else {
                    Command::new("sh")
                        .args(["-c", &cmd_str])
                        .output()
                        .map_err(|e| format!("Command failed: {}", e))?
                };

                let mut combined = Vec::new();
                combined.extend_from_slice(&output.stdout);
                combined.extend_from_slice(&output.stderr);

                self.step_index += 1;
                self.send_checkpoint(
                    self.step_index,
                    &[json!({"type": "run_command", "path": &cmd_str})],
                    50,
                );
                artifacts.push(self.create_artifact("log", &cmd_str, &combined));
            }

            _ => {
                self.log(&format!(
                    "Unknown action type: {}, treating as no-op",
                    action_type
                ));
                self.step_index += 1;
                self.send_checkpoint(
                    self.step_index,
                    &[json!({"type": action_type, "path": target})],
                    50,
                );
            }
        }

        self.send_checkpoint(
            self.step_index + 1,
            &[json!({"type": "completed", "path": target})],
            100,
        );

        Ok(artifacts)
    }

    fn handle_cancel(&self, msg: &JsonRpcNotification) {
        let reason = msg
            .params
            .as_ref()
            .and_then(|p| p.get("reason"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        self.log(&format!("Task cancelled: {}", reason));
    }

    fn run(&mut self) {
        self.log("Worker started, waiting for execute command...");

        let stdin = std::io::stdin();
        let reader = BufReader::new(stdin);

        for line in reader.lines() {
            let line = match line {
                Ok(l) => l,
                Err(e) => {
                    self.log(&format!("Read error: {}", e));
                    break;
                }
            };

            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            let msg = match serde_json::from_str::<Value>(trimmed) {
                Ok(v) => v,
                Err(e) => {
                    self.log(&format!("Invalid JSON: {} — {}", trimmed, e));
                    continue;
                }
            };

            let method = msg
                .get("method")
                .and_then(|m| m.as_str())
                .map(|s| s.to_string())
                .unwrap_or_default();

            match method.as_str() {
                "execute" => {
                    let req: JsonRpcRequest = match serde_json::from_value(msg) {
                        Ok(r) => r,
                        Err(e) => {
                            self.log(&format!("Invalid execute request: {}", e));
                            continue;
                        }
                    };
                    self.handle_execute(&req);
                }
                "cancel" => {
                    let notif: JsonRpcNotification = match serde_json::from_value(msg) {
                        Ok(n) => n,
                        Err(e) => {
                            self.log(&format!("Invalid cancel notification: {}", e));
                            continue;
                        }
                    };
                    self.handle_cancel(&notif);
                }
                "" => {
                    // Response message from Kernel, ignore
                }
                _ => {
                    if let Ok(req) = serde_json::from_value::<JsonRpcRequest>(msg) {
                        self.send_error(&req.id, -32601, &format!("Unknown method: {}", method));
                    }
                }
            }
        }

        self.log("Worker shutting down — stdin closed.");
    }
}

fn main() {
    let mut worker = WorkerProtocol::new();
    worker.run();
}
