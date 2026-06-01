use async_trait::async_trait;
use sqlx::postgres::{PgPool, PgPoolOptions};
use nexus_core::{
    NexusEvent, NexusState, SessionId, SideEffectIntent, LlmCallRecord, ArtifactRef,
    LockMode,
};
use super::store::{EventStore, StoreError};
use super::rows::{EventRow, StateRow};

pub struct PostgresEventStore {
    pool: PgPool,
}

impl PostgresEventStore {
    pub async fn new(database_url: &str) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await
            .map_err(|e| StoreError::ConnectionFailed(e.to_string()))?;

        sqlx::query(include_str!("../schema_postgres.sql"))
            .execute(&pool)
            .await
            .map_err(|e| StoreError::ConnectionFailed(format!("schema error: {}", e)))?;

        Ok(Self { pool })
    }
}

#[async_trait]
impl EventStore for PostgresEventStore {
    async fn append_event(&self, event: &NexusEvent) -> Result<(), StoreError> {
        let payload_bytes = rmp_serde::to_vec(&event.payload)
            .map_err(|e| StoreError::SerializationError(e.to_string()))?;

        sqlx::query(
            r#"INSERT INTO events (
                event_id, event_type, session_id, trace_id, parent_event_id,
                causal_vector, payload, payload_hash, event_timestamp,
                nonce, integrity_hash
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"#
        )
        .bind(&event.event_id)
        .bind(serde_json::to_string(&event.event_type).unwrap_or_default())
        .bind(event.session_id.as_bytes().as_slice())
        .bind(&event.trace_id[..])
        .bind(&event.parent_event_id)
        .bind(&event.causal_vector.to_canonical())
        .bind(&payload_bytes)
        .bind(&event.payload_hash)
        .bind(event.event_timestamp as i64)
        .bind(&event.nonce)
        .bind(&event.integrity_hash)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::ConnectionFailed(e.to_string()))?;

        Ok(())
    }

    async fn get_events(
        &self,
        session_id: SessionId,
        _since: Option<u64>,
    ) -> Result<Vec<NexusEvent>, StoreError> {
        let rows = sqlx::query_as::<_, EventRow>(
            "SELECT event_id, event_type, session_id, trace_id, parent_event_id,
             causal_vector, payload, payload_hash, event_timestamp,
             nonce, integrity_hash
             FROM events
             WHERE session_id = $1
             ORDER BY event_timestamp, event_id",
        )
        .bind(session_id.as_bytes().as_slice())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::ConnectionFailed(e.to_string()))?;

        rows.into_iter()
            .map(|r| r.to_nexus_event())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| StoreError::SerializationError(e))
    }

    async fn get_event(&self, event_id: &str) -> Result<Option<NexusEvent>, StoreError> {
        let row = sqlx::query_as::<_, EventRow>(
            "SELECT event_id, event_type, session_id, trace_id, parent_event_id,
             causal_vector, payload, payload_hash, event_timestamp,
             nonce, integrity_hash
             FROM events WHERE event_id = $1",
        )
        .bind(event_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::ConnectionFailed(e.to_string()))?;

        row.map(|r| r.to_nexus_event())
            .transpose()
            .map_err(|e| StoreError::SerializationError(e))
    }

    async fn get_state(&self, session_id: SessionId) -> Result<Option<NexusState>, StoreError> {
        let row = sqlx::query_as::<_, StateRow>(
            "SELECT session_id, version, status, intent_graph, execution_frontier,
             memory_refs, budget, checkpoint_seq, created_at, updated_at, latest_event_id
             FROM sessions WHERE session_id = $1",
        )
        .bind(session_id.as_bytes().as_slice())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::ConnectionFailed(e.to_string()))?;

        row.map(|r| r.to_nexus_state())
            .transpose()
            .map_err(|e| StoreError::SerializationError(e))
    }

    async fn update_state(
        &self,
        state: &NexusState,
        expected_version: u64,
    ) -> Result<bool, StoreError> {
        let result = sqlx::query(
            "UPDATE sessions SET
                version = $2, status = $3,
                updated_at = $4, latest_event_id = $5,
                checkpoint_seq = $6
             WHERE session_id = $1 AND version = $7",
        )
        .bind(state.session_id.as_bytes().as_slice())
        .bind(state.version as i64)
        .bind(format!("{:?}", state.status).to_lowercase())
        .bind(state.last_activity_at as i64)
        .bind(&state.latest_event_id)
        .bind(state.checkpoint_seq as i64)
        .bind(expected_version as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::ConnectionFailed(e.to_string()))?;

        Ok(result.rows_affected() > 0)
    }

    async fn record_side_effect_intent(
        &self,
        _intent: &SideEffectIntent,
    ) -> Result<(), StoreError> {
        Ok(())
    }

    async fn commit_side_effect(
        &self,
        _id: &[u8],
        _response_hash: &str,
    ) -> Result<(), StoreError> {
        Ok(())
    }

    async fn acquire_lock(
        &self,
        _resource_id: &str,
        _session_id: SessionId,
        _mode: LockMode,
    ) -> Result<bool, StoreError> {
        Ok(true)
    }

    async fn release_lock(
        &self,
        _resource_id: &str,
        _session_id: SessionId,
    ) -> Result<bool, StoreError> {
        Ok(true)
    }

    async fn record_llm_call(&self, _call: &LlmCallRecord) -> Result<(), StoreError> {
        Ok(())
    }

    async fn register_artifact(&self, _artifact: &ArtifactRef) -> Result<(), StoreError> {
        Ok(())
    }

    async fn health_check(&self) -> Result<(), StoreError> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::ConnectionFailed(e.to_string()))?;
        Ok(())
    }
}
