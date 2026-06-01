"""
Nexus Runtime — Python Worker
JSON-RPC 2.0 over stdio (NDJSON framing)

Usage:
    python main.py

Protocol:
    - Reads JSON-RPC messages from stdin (newline-delimited)
    - Writes JSON-RPC responses/notifications to stdout
    - No network access, no persistent state
"""
import sys
import json
import time
import hashlib
import traceback
from typing import Any, Dict, List, Optional


class WorkerProtocol:
    """Implements the Nexus Worker JSON-RPC 2.0 protocol over stdio."""

    def __init__(self):
        self.task_id: Optional[str] = None
        self.session_id: Optional[str] = None
        self.capabilities: List[str] = []
        self.step_index: int = 0

    def log(self, message: str):
        """Write log to stderr (stdout is reserved for JSON-RPC)."""
        print(f"[Nexus Worker] {message}", file=sys.stderr, flush=True)

    def read_message(self) -> Optional[Dict[str, Any]]:
        """Read a single JSON-RPC message from stdin."""
        try:
            line = sys.stdin.readline()
            if not line:
                return None
            return json.loads(line.strip())
        except json.JSONDecodeError as e:
            self.log(f"JSON parse error: {e}")
            return None

    def send_message(self, message: Dict[str, Any]):
        """Send a JSON-RPC message to stdout."""
        sys.stdout.write(json.dumps(message) + "\n")
        sys.stdout.flush()

    def send_checkpoint(self, step_index: int, actions: List[Dict], progress_percent: int):
        """Send a checkpoint notification to the Kernel."""
        self.send_message({
            "jsonrpc": "2.0",
            "method": "checkpoint",
            "params": {
                "task_id": self.task_id,
                "step_index": step_index,
                "actions": actions,
                "progress_percent": progress_percent
            }
        })
        self.log(f"Checkpoint at step {step_index} ({progress_percent}%)")

    def send_progress(self, percent: int, current_step: str, sub_steps: List[Dict]):
        """Send a progress notification."""
        self.send_message({
            "jsonrpc": "2.0",
            "method": "progress",
            "params": {
                "task_id": self.task_id,
                "percent": percent,
                "current_step": current_step,
                "sub_steps": sub_steps
            }
        })

    def send_result(self, request_id: Any, status: str, artifacts: List[Dict], metrics: Dict):
        """Send an execution result."""
        self.send_message({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "status": status,
                "artifacts": artifacts,
                "metrics": metrics
            }
        })

    def send_error(self, request_id: Any, code: int, message: str, data: Optional[Dict] = None):
        """Send an error response."""
        error_msg: Dict[str, Any] = {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": code,
                "message": message
            }
        }
        if data:
            error_msg["error"]["data"] = data
        self.send_message(error_msg)

    def handle_execute(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        """Handle the 'execute' method from the Kernel."""
        params = msg.get("params", {})
        self.task_id = params.get("task_id", "unknown")
        self.session_id = params.get("session_id", "unknown")
        self.capabilities = params.get("capabilities", [])
        self.step_index = params.get("from_step", 0)

        intent = params.get("intent", {})
        inputs = params.get("inputs", [])

        self.log(f"Execute task: {self.task_id}")
        self.log(f"Intent: {intent.get('action_type', 'unknown')} -> {intent.get('target', 'unknown')}")
        self.log(f"Capabilities: {self.capabilities}")

        try:
            # Execute the intent
            result = self.execute_intent(intent, inputs)
            return result
        except Exception as e:
            self.log(f"Execution failed: {e}")
            traceback.print_exc(file=sys.stderr)
            return {"error": str(e)}

    def execute_intent(self, intent: Dict, inputs: List[Dict]) -> Dict[str, Any]:
        """Execute the actual intent. Override this for custom behavior."""
        action_type = intent.get("action_type", "")
        target = intent.get("target", "")
        params = intent.get("parameters", {})

        artifacts = []
        start_time = time.time()

        # Dispatch based on action type
        if action_type == "read_file":
            try:
                with open(target, "r") as f:
                    content = f.read()
                artifacts.append(self._create_artifact("file", target, content))
                self.step_index += 1
                self.send_checkpoint(self.step_index, [{"type": "read_file", "path": target}], 50)
            except FileNotFoundError:
                return {"error": f"File not found: {target}"}

        elif action_type == "write_file":
            content = params.get("content", "")
            try:
                with open(target, "w") as f:
                    f.write(content)
                artifacts.append(self._create_artifact("file", target, content))
                self.step_index += 1
                self.send_checkpoint(self.step_index, [{"type": "write_file", "path": target}], 50)
            except Exception as e:
                return {"error": f"Write failed: {e}"}

        elif action_type == "grep":
            pattern = params.get("pattern", "")
            try:
                with open(target, "r") as f:
                    lines = f.readlines()
                matches = [line.rstrip() for line in lines if pattern in line]
                result_text = "\n".join(matches)
                artifacts.append(self._create_artifact("text", target, result_text))
                self.step_index += 1
                self.send_checkpoint(self.step_index, [{"type": "grep", "path": target}], 50)
            except FileNotFoundError:
                return {"error": f"File not found: {target}"}

        elif action_type == "run_command":
            import subprocess
            cmd = params.get("command", target)
            try:
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=60)
                output = result.stdout + result.stderr
                artifacts.append(self._create_artifact("log", f"cmd:{cmd}", output))
                self.step_index += 1
                self.send_checkpoint(self.step_index, [{"type": "run_command", "path": cmd}], 50)
            except subprocess.TimeoutExpired:
                return {"error": f"Command timed out: {cmd}"}
            except Exception as e:
                return {"error": f"Command failed: {e}"}

        else:
            self.log(f"Unknown action type: {action_type}, treating as no-op")
            self.step_index += 1
            self.send_checkpoint(self.step_index, [{"type": action_type, "path": target}], 50)

        duration_ms = int((time.time() - start_time) * 1000)

        self.send_checkpoint(self.step_index + 1, [{"type": "completed", "path": target}], 100)

        return {
            "status": "completed",
            "artifacts": artifacts,
            "metrics": {
                "duration_ms": duration_ms,
                "tokens_consumed": 0,
                "cost_cents": 0
            }
        }

    def _create_artifact(self, kind: str, path: str, content: str) -> Dict:
        """Create an artifact reference from content."""
        content_bytes = content.encode("utf-8") if isinstance(content, str) else content
        blake3_hash = hashlib.blake2b(content_bytes).hexdigest()
        artifact_id = f"art_{blake3_hash[:16]}"

        return {
            "id": artifact_id,
            "uri": f"vault://artifacts/{blake3_hash[:16]}",
            "blake3": blake3_hash,
            "size_bytes": len(content_bytes),
            "kind": kind,
            "metadata": {
                "path": path,
                "encoding": "utf-8"
            }
        }

    def handle_cancel(self, msg: Dict[str, Any]):
        """Handle cancellation from the Kernel."""
        params = msg.get("params", {})
        reason = params.get("reason", "unknown")
        self.log(f"Task cancelled: {reason}")

    def run(self):
        """Main event loop — read from stdin, process, write to stdout."""
        self.log("Worker started, waiting for execute command...")

        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue

            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                self.log(f"Invalid JSON: {line}")
                continue

            method = msg.get("method", "")
            msg_id = msg.get("id")

            try:
                if method == "execute":
                    result = self.handle_execute(msg)
                    if "error" in result:
                        self.send_error(msg_id, -32603, result["error"])
                    else:
                        self.send_result(msg_id, result["status"], result.get("artifacts", []), result.get("metrics", {}))
                elif method == "cancel":
                    self.handle_cancel(msg)
                else:
                    self.send_error(msg_id, -32601, f"Unknown method: {method}")
            except Exception as e:
                self.log(f"Unexpected error: {e}")
                traceback.print_exc(file=sys.stderr)
                self.send_error(msg_id, -32603, str(e))

        self.log("Worker shutting down — stdin closed.")


if __name__ == "__main__":
    worker = WorkerProtocol()
    worker.run()
