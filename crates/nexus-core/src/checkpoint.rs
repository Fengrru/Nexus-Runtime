use crate::types::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Checkpoint {
    pub checkpoint_id: String,
    pub session_id: SessionId,
    pub step_index: u64,
    pub total_actions: u64,
    pub replay_actions: Vec<ReplayAction>,
    pub artifact_refs: Vec<ArtifactRef>,
    pub handle_registry: Vec<HandleRecord>,
    pub determinism_context: DeterminismContext,
    pub created_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReplayAction {
    ReadFile {
        path: String,
        expected_hash: String,
    },
    EditFile {
        path: String,
        search: String,
        replace: String,
        expected_count: u32,
    },
    RunCommand {
        command: String,
        args: Vec<String>,
        env: BTreeMap<String, String>,
    },
    LlmCall {
        request_id: String,
        model: String,
        prompt_hash: String,
        response_artifact: ArtifactRef,
    },
    McpInvoke {
        capability: String,
        args_hash: String,
        result_artifact: Option<ArtifactRef>,
    },
    GitCommit {
        message: String,
        files: Vec<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HandleRecord {
    pub handle_type: String,
    pub reacquire_command: String,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeterminismContext {
    pub seed: u64,
    pub model_version: String,
    pub input_hash: String,
    pub checkpoint_format_version: u16,
    pub worker_type: WorkerType,
}
