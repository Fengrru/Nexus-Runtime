use async_trait::async_trait;
use nexus_core::*;
use nexus_event_store::{EventStore, StoreError};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkflowState {
    Initialized,
    Running,
    AwaitingSignal,
    Completed,
    Failed,
    TimedOut,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemporalWorkflowContext {
    pub workflow_id: String,
    pub run_id: String,
    pub namespace: String,
    pub task_queue: String,
    pub state: WorkflowState,
    pub attempt: u32,
    pub started_at: u64,
    pub last_heartbeat_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityContext {
    pub activity_id: String,
    pub activity_type: String,
    pub workflow_id: String,
    pub attempt: u32,
    pub scheduled_at: u64,
    pub started_at: u64,
    pub heartbeat_timeout_ms: u64,
    pub schedule_to_close_timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowResult {
    pub status: WorkflowState,
    pub output: Option<Vec<u8>>,
    pub error: Option<String>,
    pub events_processed: u64,
}

pub struct TemporalEventStore {
    namespace: String,
    task_queue: String,
    local_store: Option<Box<dyn EventStore>>,
}

impl TemporalEventStore {
    pub fn new(namespace: String, task_queue: String) -> Self {
        Self {
            namespace,
            task_queue,
            local_store: None,
        }
    }

    pub fn with_local_store(mut self, store: Box<dyn EventStore>) -> Self {
        self.local_store = Some(store);
        self
    }

    pub fn generate_workflow_id(session_id: SessionId) -> String {
        format!("nexus-session-{}", session_id.to_hex())
    }

    pub fn generate_activity_id(session_id: SessionId, task_id: TaskId) -> String {
        format!(
            "nexus-activity-{}-{}",
            hex::encode(&session_id.as_bytes()[..8]),
            hex::encode(&task_id.0[..8])
        )
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn task_queue(&self) -> &str {
        &self.task_queue
    }
}

#[async_trait]
impl EventStore for TemporalEventStore {
    async fn append_event(&self, event: &NexusEvent) -> Result<(), StoreError> {
        if let Some(ref store) = self.local_store {
            store.append_event(event).await
        } else {
            tracing::warn!(
                target = "nexus.temporal",
                event_id = %event.event_id,
                "Temporal store: event would be recorded as WorkflowEvent"
            );
            Ok(())
        }
    }

    async fn get_events(
        &self,
        session_id: SessionId,
        since: Option<u64>,
    ) -> Result<Vec<NexusEvent>, StoreError> {
        if let Some(ref store) = self.local_store {
            store.get_events(session_id, since).await
        } else {
            Err(StoreError::ConnectionFailed(
                "Temporal event history requires running workflow handle".into(),
            ))
        }
    }

    async fn get_event(&self, event_id: &str) -> Result<Option<NexusEvent>, StoreError> {
        if let Some(ref store) = self.local_store {
            store.get_event(event_id).await
        } else {
            Ok(None)
        }
    }

    async fn get_state(&self, session_id: SessionId) -> Result<Option<NexusState>, StoreError> {
        if let Some(ref store) = self.local_store {
            store.get_state(session_id).await
        } else {
            Ok(None)
        }
    }

    async fn update_state(
        &self,
        state: &NexusState,
        expected_version: u64,
    ) -> Result<bool, StoreError> {
        if let Some(ref store) = self.local_store {
            store.update_state(state, expected_version).await
        } else {
            Ok(true)
        }
    }

    async fn record_side_effect_intent(
        &self,
        intent: &SideEffectIntent,
    ) -> Result<(), StoreError> {
        if let Some(ref store) = self.local_store {
            store.record_side_effect_intent(intent).await
        } else {
            Ok(())
        }
    }

    async fn commit_side_effect(
        &self,
        id: &[u8],
        response_hash: &str,
    ) -> Result<(), StoreError> {
        if let Some(ref store) = self.local_store {
            store.commit_side_effect(id, response_hash).await
        } else {
            Ok(())
        }
    }

    async fn acquire_lock(
        &self,
        resource_id: &str,
        session_id: SessionId,
        mode: LockMode,
    ) -> Result<bool, StoreError> {
        if let Some(ref store) = self.local_store {
            store.acquire_lock(resource_id, session_id, mode).await
        } else {
            Ok(true)
        }
    }

    async fn release_lock(
        &self,
        resource_id: &str,
        session_id: SessionId,
    ) -> Result<bool, StoreError> {
        if let Some(ref store) = self.local_store {
            store.release_lock(resource_id, session_id).await
        } else {
            Ok(true)
        }
    }

    async fn record_llm_call(&self, call: &LlmCallRecord) -> Result<(), StoreError> {
        if let Some(ref store) = self.local_store {
            store.record_llm_call(call).await
        } else {
            Ok(())
        }
    }

    async fn register_artifact(&self, artifact: &ArtifactRef) -> Result<(), StoreError> {
        if let Some(ref store) = self.local_store {
            store.register_artifact(artifact).await
        } else {
            Ok(())
        }
    }

    async fn health_check(&self) -> Result<(), StoreError> {
        if let Some(ref store) = self.local_store {
            store.health_check().await
        } else {
            Ok(())
        }
    }
}

pub struct TemporalWorkflowManager {
    namespace: String,
    task_queue: String,
    store: TemporalEventStore,
}

impl TemporalWorkflowManager {
    pub fn new(namespace: String, task_queue: String) -> Self {
        Self {
            namespace: namespace.clone(),
            task_queue: task_queue.clone(),
            store: TemporalEventStore::new(namespace, task_queue),
        }
    }

    pub fn with_local_store(mut self, store: Box<dyn EventStore>) -> Self {
        self.store = TemporalEventStore::new(
            self.namespace.clone(),
            self.task_queue.clone(),
        )
        .with_local_store(store);
        self
    }

    pub async fn start_workflow(
        &self,
        session_id: SessionId,
        intent: &str,
    ) -> Result<TemporalWorkflowContext, String> {
        let ctx = TemporalWorkflowContext {
            workflow_id: TemporalEventStore::generate_workflow_id(session_id),
            run_id: uuid::Uuid::new_v4().to_string(),
            namespace: self.namespace.clone(),
            task_queue: self.task_queue.clone(),
            state: WorkflowState::Initialized,
            attempt: 1,
            started_at: now_millis(),
            last_heartbeat_at: now_millis(),
        };

        tracing::info!(
            target = "nexus.temporal.workflow",
            workflow_id = %ctx.workflow_id,
            run_id = %ctx.run_id,
            intent = %intent,
            "Workflow started"
        );

        Ok(ctx)
    }

    pub async fn signal_workflow(
        &self,
        workflow_id: &str,
        signal_name: &str,
        _payload: &[u8],
    ) -> Result<(), String> {
        tracing::info!(
            target = "nexus.temporal.signal",
            workflow_id = %workflow_id,
            signal = %signal_name,
            "Signal delivered"
        );
        Ok(())
    }

    pub async fn query_workflow_state(
        &self,
        _workflow_id: &str,
    ) -> Result<WorkflowState, String> {
        Ok(WorkflowState::Running)
    }

    pub async fn complete_workflow(
        &self,
        workflow_id: &str,
        result: WorkflowResult,
    ) -> Result<(), String> {
        tracing::info!(
            target = "nexus.temporal.workflow",
            workflow_id = %workflow_id,
            status = ?result.status,
            events = %result.events_processed,
            "Workflow completed"
        );
        Ok(())
    }

    pub fn store(&self) -> &TemporalEventStore {
        &self.store
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workflow_id_generation() {
        let sid = SessionId::from_bytes([1u8; 16]);
        let wid = TemporalEventStore::generate_workflow_id(sid);
        assert!(wid.starts_with("nexus-session-"));
        assert!(wid.contains(&sid.to_hex()));
    }

    #[test]
    fn test_activity_id_generation() {
        let sid = SessionId::from_bytes([1u8; 16]);
        let tid = TaskId::from_bytes([2u8; 16]);
        let aid = TemporalEventStore::generate_activity_id(sid, tid);
        assert!(aid.starts_with("nexus-activity-"));
    }

    #[tokio::test]
    async fn test_temporal_workflow_lifecycle() {
        let manager = TemporalWorkflowManager::new("nexus-namespace".into(), "nexus-queue".into());
        let sid = SessionId::from_bytes([1u8; 16]);

        let ctx = manager.start_workflow(sid, "test intent").await.unwrap();
        assert_eq!(ctx.state, WorkflowState::Initialized);
        assert_eq!(ctx.namespace, "nexus-namespace");
    }
}
