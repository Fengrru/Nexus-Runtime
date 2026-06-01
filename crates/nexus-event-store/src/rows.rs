use nexus_core::{NexusEvent, NexusState, SessionId, CausalVector};
use sqlx::FromRow;

#[derive(Debug, FromRow)]
#[allow(dead_code)]
pub struct EventRow {
    pub event_id: String,
    pub event_type: String,
    pub session_id: Vec<u8>,
    pub trace_id: Vec<u8>,
    pub parent_event_id: Option<String>,
    pub causal_vector: String,
    pub payload: Vec<u8>,
    pub payload_hash: String,
    pub event_timestamp: i64,
    pub nonce: String,
    pub integrity_hash: String,
}

impl EventRow {
    pub fn to_nexus_event(self) -> Result<NexusEvent, String> {
        let mut sid_bytes = [0u8; 16];
        if self.session_id.len() >= 16 {
            sid_bytes.copy_from_slice(&self.session_id[..16]);
        }
        let session_id = SessionId::from_bytes(sid_bytes);

        let mut tid_bytes = [0u8; 16];
        if self.trace_id.len() >= 16 {
            tid_bytes.copy_from_slice(&self.trace_id[..16]);
        }

        let causal_vector = CausalVector::from_canonical(&self.causal_vector)
            .map_err(|e| format!("causal_vector parse: {}", e))?;

        let event_type: nexus_core::EventType = serde_json::from_str(&self.event_type)
            .map_err(|e| format!("event_type parse: {}", e))?;

        Ok(NexusEvent {
            event_id: self.event_id,
            event_type,
            session_id,
            trace_id: tid_bytes,
            parent_event_id: self.parent_event_id,
            causal_vector,
            payload: self.payload,
            payload_hash: self.payload_hash,
            event_timestamp: self.event_timestamp as u64,
            nonce: self.nonce,
            integrity_hash: self.integrity_hash,
        })
    }
}

#[derive(Debug, FromRow)]
#[allow(dead_code)]
pub struct StateRow {
    pub session_id: Vec<u8>,
    pub version: i64,
    pub status: String,
    pub intent_graph: Vec<u8>,
    pub execution_frontier: Vec<u8>,
    pub memory_refs: Vec<u8>,
    pub budget: Vec<u8>,
    pub checkpoint_seq: i64,
    pub created_at: i64,
    pub updated_at: i64,
    pub latest_event_id: String,
}

impl StateRow {
    pub fn to_nexus_state(self) -> Result<NexusState, String> {
        let mut sid_bytes = [0u8; 16];
        if self.session_id.len() >= 16 {
            sid_bytes.copy_from_slice(&self.session_id[..16]);
        }
        let session_id = SessionId::from_bytes(sid_bytes);

        let status = match self.status.as_str() {
            "created" => nexus_core::SessionStatus::Created,
            "intake" => nexus_core::SessionStatus::Intake,
            "planning" => nexus_core::SessionStatus::Planning,
            "planned" => nexus_core::SessionStatus::Planned,
            "executing" => nexus_core::SessionStatus::Executing,
            "checkpointing" => nexus_core::SessionStatus::Checkpointing,
            "blocked" => nexus_core::SessionStatus::Blocked,
            "converging" => nexus_core::SessionStatus::Converging,
            "reflecting" => nexus_core::SessionStatus::Reflecting,
            "completed" => nexus_core::SessionStatus::Completed,
            "failed" => nexus_core::SessionStatus::Failed,
            "archived" => nexus_core::SessionStatus::Archived,
            _ => nexus_core::SessionStatus::Created,
        };

        Ok(NexusState {
            session_id,
            version: self.version as u64,
            status,
            causal_vector: CausalVector::new(),
            intent_graph: Default::default(),
            execution_frontier: Default::default(),
            memory_refs: vec![],
            memory_graph: Default::default(),
            budget: Default::default(),
            retry_policy: Default::default(),
            checkpoint_seq: self.checkpoint_seq as u64,
            created_at: self.created_at as u64,
            last_activity_at: self.updated_at as u64,
            latest_event_id: self.latest_event_id,
        })
    }
}
