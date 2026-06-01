use async_trait::async_trait;
use nexus_core::{NexusEvent, NexusState, SessionId, SideEffectIntent, LlmCallRecord, ArtifactRef, LockMode};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Optimistic lock conflict: expected version {expected}, found {found}")]
    OptimisticLockConflict { expected: u64, found: u64 },

    #[error("Integrity check failed: {0}")]
    IntegrityCheckFailed(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Event not found: {0}")]
    EventNotFound(String),

    #[error("Store is read-only")]
    ReadOnly,
}

#[async_trait]
pub trait EventStore: Send + Sync + 'static {
    async fn append_event(&self, event: &NexusEvent) -> Result<(), StoreError>;

    async fn append_events(&self, events: &[NexusEvent]) -> Result<(), StoreError> {
        for event in events {
            self.append_event(event).await?;
        }
        Ok(())
    }

    async fn get_events(
        &self,
        session_id: SessionId,
        since: Option<u64>,
    ) -> Result<Vec<NexusEvent>, StoreError>;

    async fn get_event(&self, event_id: &str) -> Result<Option<NexusEvent>, StoreError>;

    async fn get_state(&self, session_id: SessionId) -> Result<Option<NexusState>, StoreError>;

    async fn update_state(
        &self,
        state: &NexusState,
        expected_version: u64,
    ) -> Result<bool, StoreError>;

    async fn record_side_effect_intent(
        &self,
        intent: &SideEffectIntent,
    ) -> Result<(), StoreError>;

    async fn commit_side_effect(
        &self,
        id: &[u8],
        response_hash: &str,
    ) -> Result<(), StoreError>;

    async fn acquire_lock(
        &self,
        resource_id: &str,
        session_id: SessionId,
        mode: LockMode,
    ) -> Result<bool, StoreError>;

    async fn release_lock(
        &self,
        resource_id: &str,
        session_id: SessionId,
    ) -> Result<bool, StoreError>;

    async fn record_llm_call(&self, call: &LlmCallRecord) -> Result<(), StoreError>;

    async fn register_artifact(&self, artifact: &ArtifactRef) -> Result<(), StoreError>;

    async fn health_check(&self) -> Result<(), StoreError>;
}
