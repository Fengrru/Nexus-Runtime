#![deny(clippy::disallowed_types)]

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub id: Option<Value>,
    pub method: String,
    pub params: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub id: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcNotification {
    pub jsonrpc: String,
    pub method: String,
    pub params: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteParams {
    pub task_id: String,
    pub session_id: String,
    pub intent: TaskIntentPayload,
    pub inputs: Vec<InputArtifact>,
    pub from_step: u64,
    pub capabilities: Vec<String>,
    pub timeout_ms: u64,
    pub token_budget: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskIntentPayload {
    pub action_type: String,
    pub target: String,
    pub parameters: BTreeMap<String, String>,
    pub constraints: Vec<ConstraintPayload>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintPayload {
    #[serde(rename = "type")]
    pub constraint_type: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputArtifact {
    pub artifact_ref: ArtifactPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactPayload {
    pub id: String,
    pub uri: String,
    pub blake3: String,
    pub size_bytes: u64,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub metadata: Option<BTreeMap<String, Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointParams {
    pub task_id: String,
    pub step_index: u64,
    pub actions: Vec<ActionPayload>,
    pub artifacts: Vec<ArtifactPayload>,
    #[serde(default)]
    pub progress_percent: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionPayload {
    #[serde(rename = "type")]
    pub action_type: String,
    pub path: Option<String>,
    pub artifact: Option<ArtifactPayload>,
    pub search: Option<String>,
    pub replace: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressParams {
    pub task_id: String,
    pub percent: u32,
    pub current_step: String,
    #[serde(default)]
    pub sub_steps: Vec<SubStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubStep {
    pub name: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultParams {
    pub status: String,
    pub artifacts: Vec<ArtifactPayload>,
    pub metrics: MetricsPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsPayload {
    pub duration_ms: u64,
    pub tokens_consumed: u64,
    pub cost_cents: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelParams {
    pub task_id: String,
    pub reason: String,
    pub timeout_ms: u64,
}

pub struct RpcCodec;

impl RpcCodec {
    pub fn encode_request(req: &JsonRpcRequest) -> Result<String, serde_json::Error> {
        let mut json = serde_json::to_string(req)?;
        json.push('\n');
        Ok(json)
    }

    pub fn encode_response(resp: &JsonRpcResponse) -> Result<String, serde_json::Error> {
        let mut json = serde_json::to_string(resp)?;
        json.push('\n');
        Ok(json)
    }

    pub fn encode_notification(notif: &JsonRpcNotification) -> Result<String, serde_json::Error> {
        let mut json = serde_json::to_string(notif)?;
        json.push('\n');
        Ok(json)
    }

    pub fn decode_message(data: &str) -> Result<JsonRpcMessage, serde_json::Error> {
        let value: Value = serde_json::from_str(data)?;

        if value.get("method").is_some() && value.get("id").is_none() {
            let notif: JsonRpcNotification = serde_json::from_value(value)?;
            Ok(JsonRpcMessage::Notification(notif))
        } else if value.get("error").is_some() || value.get("result").is_some() {
            let resp: JsonRpcResponse = serde_json::from_value(value)?;
            Ok(JsonRpcMessage::Response(resp))
        } else {
            let req: JsonRpcRequest = serde_json::from_value(value)?;
            Ok(JsonRpcMessage::Request(req))
        }
    }

    pub fn canonicalize_worker_payload(value: &Value) -> Value {
        match value {
            Value::Object(obj) => {
                let mut sorted: BTreeMap<String, Value> = BTreeMap::new();
                for (k, v) in obj {
                    sorted.insert(k.clone(), Self::canonicalize_worker_payload(v));
                }
                Value::Object(sorted.into_iter().collect())
            }
            Value::Array(arr) => {
                Value::Array(arr.iter().map(Self::canonicalize_worker_payload).collect())
            }
            other => other.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum JsonRpcMessage {
    Request(JsonRpcRequest),
    Response(JsonRpcResponse),
    Notification(JsonRpcNotification),
}

pub const ERROR_CODES: &[(&str, i32)] = &[
    ("ParseError", -32700),
    ("InvalidRequest", -32600),
    ("MethodNotFound", -32601),
    ("InvalidParams", -32602),
    ("InternalError", -32603),
    ("CapabilityDenied", -32001),
    ("BudgetExceeded", -32002),
    ("Timeout", -32003),
    ("Cancelled", -32004),
    ("SandboxViolation", -32005),
];

pub fn make_error_response(id: Option<Value>, code: i32, message: &str) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0".into(),
        id,
        result: None,
        error: Some(JsonRpcError {
            code,
            message: message.into(),
            data: None,
        }),
    }
}

pub fn make_success_response(id: Option<Value>, result: Value) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0".into(),
        id,
        result: Some(result),
        error: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_decode_execute_request() {
        let msg = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "execute",
            "params": {
                "task_id": "task_001",
                "session_id": "sess_001",
                "intent": {
                    "action_type": "refactor",
                    "target": "auth",
                    "parameters": {},
                    "constraints": []
                },
                "inputs": [],
                "from_step": 0,
                "capabilities": ["fs:read:/src"],
                "timeout_ms": 300000,
                "token_budget": 100000
            }
        });

        let result = RpcCodec::decode_message(&msg.to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_encode_request() {
        let req = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: Some(json!(1)),
            method: "checkpoint".into(),
            params: Some(json!({"task_id": "t1"})),
        };
        let encoded = RpcCodec::encode_request(&req).unwrap();
        assert!(encoded.ends_with('\n'));
    }

    #[test]
    fn test_canonicalize_payload() {
        let input = json!({"b": 2, "a": 1});
        let canonical = RpcCodec::canonicalize_worker_payload(&input);
        let keys: Vec<String> = canonical.as_object().unwrap().keys().cloned().collect();
        assert_eq!(keys, vec!["a", "b"]);
    }
}
