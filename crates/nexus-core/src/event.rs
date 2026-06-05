use crate::protocol;
use crate::types::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventType {
    // Intake phase
    IntentReceived {
        raw_input: String,
        source: String,
    },
    IntentParsed {
        intent_graph: IntentGraph,
    },

    // Planning phase
    PlanProposed {
        plan: ExecutionPlan,
        model: String,
        prompt_tokens: u64,
        completion_tokens: u64,
    },
    PlanCommitted {
        frontier: Frontier,
    },
    PlanRejected {
        reason: String,
    },

    // Execution phase
    DependenciesMet,
    FrontierValidated {
        validation_result: ValidationResult,
    },
    WorkerDispatched {
        worker_id: String,
        task_id: TaskId,
        worker_type: WorkerType,
    },
    WorkerStarted {
        worker_id: String,
        task_id: TaskId,
        pid: u32,
    },
    WorkerCheckpoint {
        task_id: TaskId,
        step_index: u64,
        actions: Vec<Action>,
        artifacts: Vec<ArtifactRef>,
    },
    WorkerCompleted {
        worker_id: String,
        task_id: TaskId,
        result: WorkerResult,
        duration_ms: u64,
    },
    WorkerFailed {
        worker_id: String,
        task_id: TaskId,
        error: String,
        error_code: ErrorCode,
        retry_count: u32,
    },

    // Convergence phase
    ConvergeStarted {
        task_ids: Vec<TaskId>,
    },
    ConvergeComplete {
        merged_result: WorkerResult,
    },

    // Reflection phase
    ReflectionStarted {
        checkpoint_seq: u64,
    },
    ReflectionComplete {
        evaluation: Evaluation,
        memory_delta: Vec<MemoryDelta>,
    },
    MemoryConsolidated {
        memory_ids: Vec<String>,
    },

    // Side effects
    SideEffectIntent {
        effect: SideEffectIntent,
    },
    SideEffectCommitted {
        effect_id: String,
        result_hash: String,
        committed_at: u64,
    },
    SideEffectCompensated {
        effect_id: String,
        compensation_result: String,
    },

    // Governance
    HumanApprovalRequested {
        action: Action,
        reason: String,
        timeout_ms: u64,
    },
    HumanApproved {
        approver: String,
        approved_at: u64,
    },
    HumanRejected {
        rejecter: String,
        reason: String,
    },
    PolicyDecision {
        policy_id: String,
        decision: PolicyDecision,
        latency_ms: u64,
    },

    // Session lifecycle
    SessionSuspended {
        reason: String,
    },
    SessionResumed {
        from_checkpoint: u64,
        inherited_memories: Vec<String>,
    },
    SessionMigrated {
        from: SessionId,
        to: SessionId,
        export_hash: String,
    },
    SessionArchived {
        reason: String,
        final_status: SessionStatus,
    },
}

impl EventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventType::IntentReceived { .. } => "intent_received",
            EventType::IntentParsed { .. } => "intent_parsed",
            EventType::PlanProposed { .. } => "plan_proposed",
            EventType::PlanCommitted { .. } => "plan_committed",
            EventType::PlanRejected { .. } => "plan_rejected",
            EventType::DependenciesMet => "dependencies_met",
            EventType::FrontierValidated { .. } => "frontier_validated",
            EventType::WorkerDispatched { .. } => "worker_dispatched",
            EventType::WorkerStarted { .. } => "worker_started",
            EventType::WorkerCheckpoint { .. } => "worker_checkpoint",
            EventType::WorkerCompleted { .. } => "worker_completed",
            EventType::WorkerFailed { .. } => "worker_failed",
            EventType::ConvergeStarted { .. } => "converge_started",
            EventType::ConvergeComplete { .. } => "converge_complete",
            EventType::ReflectionStarted { .. } => "reflection_started",
            EventType::ReflectionComplete { .. } => "reflection_complete",
            EventType::MemoryConsolidated { .. } => "memory_consolidated",
            EventType::SideEffectIntent { .. } => "side_effect_intent",
            EventType::SideEffectCommitted { .. } => "side_effect_committed",
            EventType::SideEffectCompensated { .. } => "side_effect_compensated",
            EventType::HumanApprovalRequested { .. } => "human_approval_requested",
            EventType::HumanApproved { .. } => "human_approved",
            EventType::HumanRejected { .. } => "human_rejected",
            EventType::PolicyDecision { .. } => "policy_decision",
            EventType::SessionSuspended { .. } => "session_suspended",
            EventType::SessionResumed { .. } => "session_resumed",
            EventType::SessionMigrated { .. } => "session_migrated",
            EventType::SessionArchived { .. } => "session_archived",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NexusEvent {
    pub event_id: String,
    pub event_type: EventType,
    pub session_id: SessionId,
    pub trace_id: [u8; 16],
    pub parent_event_id: Option<String>,
    pub causal_vector: CausalVector,
    pub payload: Vec<u8>,
    pub payload_hash: String,
    pub event_timestamp: u64,
    pub nonce: String,
    pub integrity_hash: String,
}

impl NexusEvent {
    pub fn new(
        event_type: EventType,
        session_id: SessionId,
        causal_vector: CausalVector,
        parent_event_id: Option<String>,
    ) -> Self {
        let payload_bytes = protocol::serialize_deterministic(&event_type).unwrap_or_default();
        let payload_hash = protocol::compute_hash(&payload_bytes);
        let event_timestamp = now_millis();
        let event_id = generate_event_id();
        let nonce = generate_nonce();

        let mut event = Self {
            event_id,
            event_type,
            session_id,
            trace_id: generate_trace_id(),
            parent_event_id,
            causal_vector,
            payload: payload_bytes,
            payload_hash,
            event_timestamp,
            nonce,
            integrity_hash: String::new(),
        };

        event.integrity_hash = event.compute_integrity_hash();
        event
    }

    pub fn compute_integrity_hash(&self) -> String {
        let core_data = format!(
            "{}:{}:{}:{}:{}:{}",
            self.event_id,
            self.payload_hash,
            hex::encode(self.session_id.0),
            self.event_timestamp,
            self.nonce,
            self.causal_vector.to_canonical()
        );
        protocol::compute_sha256_hash(core_data.as_bytes())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Action {
    ReadFile {
        path: String,
        artifact: ArtifactRef,
    },
    EditFile {
        path: String,
        search: String,
        replace: String,
        artifact: ArtifactRef,
    },
    RunCommand {
        command: String,
        args: Vec<String>,
        env: BTreeMap<String, String>,
    },
    GitCommit {
        message: String,
        files: Vec<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PolicyDecision {
    pub allowed: bool,
    pub reason: String,
    pub policy_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LlmCallRecordRow {
    pub request_id: String,
    pub session_id: Vec<u8>,
    pub event_id: String,
    pub model: String,
    pub prompt_hash: String,
    pub response_hash: Option<String>,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cost_usd_cents: i64,
    pub status: String,
    pub created_at: i64,
}
