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
        .bind(event.causal_vector.to_canonical())
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

    async fn append_events(&self, events: &[NexusEvent]) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await
            .map_err(|e| StoreError::ConnectionFailed(e.to_string()))?;

        for event in events {
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
            .bind(event.causal_vector.to_canonical())
            .bind(&payload_bytes)
            .bind(&event.payload_hash)
            .bind(event.event_timestamp as i64)
            .bind(&event.nonce)
            .bind(&event.integrity_hash)
            .execute(&mut *tx)
            .await
            .map_err(|e| StoreError::ConnectionFailed(e.to_string()))?;
        }

        tx.commit().await
            .map_err(|e| StoreError::ConnectionFailed(e.to_string()))?;

        Ok(())
    }

    async fn get_events(
        &self,
        session_id: SessionId,
        since: Option<u64>,
    ) -> Result<Vec<NexusEvent>, StoreError> {
        let rows = if let Some(since_ts) = since {
            sqlx::query_as::<_, EventRow>(
                "SELECT event_id, event_type, session_id, trace_id, parent_event_id,
                 causal_vector, payload, payload_hash, event_timestamp,
                 nonce, integrity_hash
                 FROM events
                 WHERE session_id = $1 AND event_timestamp > $2
                 ORDER BY event_timestamp, event_id",
            )
            .bind(session_id.as_bytes().as_slice())
            .bind(since_ts as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::ConnectionFailed(e.to_string()))?
        } else {
            sqlx::query_as::<_, EventRow>(
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
            .map_err(|e| StoreError::ConnectionFailed(e.to_string()))?
        };

        rows.into_iter()
            .map(|r| r.to_nexus_event())
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::SerializationError)
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
            .map_err(StoreError::SerializationError)
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
            .map_err(StoreError::SerializationError)
    }

    async fn update_state(
        &self,
        state: &NexusState,
        expected_version: u64,
    ) -> Result<bool, StoreError> {
        let intent_graph_bytes = rmp_serde::to_vec(&state.intent_graph)
            .map_err(|e| StoreError::SerializationError(e.to_string()))?;
        let frontier_bytes = rmp_serde::to_vec(&state.execution_frontier)
            .map_err(|e| StoreError::SerializationError(e.to_string()))?;
        let memory_refs_bytes = rmp_serde::to_vec(&state.memory_refs)
            .map_err(|e| StoreError::SerializationError(e.to_string()))?;
        let budget_bytes = rmp_serde::to_vec(&state.budget)
            .map_err(|e| StoreError::SerializationError(e.to_string()))?;

        let result = sqlx::query(
            "INSERT INTO sessions (
                session_id, version, status, intent_graph, execution_frontier,
                memory_refs, budget, checkpoint_seq, created_at, updated_at, latest_event_id
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (session_id) DO UPDATE SET
                version = EXCLUDED.version,
                status = EXCLUDED.status,
                intent_graph = EXCLUDED.intent_graph,
                execution_frontier = EXCLUDED.execution_frontier,
                memory_refs = EXCLUDED.memory_refs,
                budget = EXCLUDED.budget,
                checkpoint_seq = EXCLUDED.checkpoint_seq,
                updated_at = EXCLUDED.updated_at,
                latest_event_id = EXCLUDED.latest_event_id",
        )
        .bind(state.session_id.as_bytes().as_slice())
        .bind(state.version as i64)
        .bind(format!("{:?}", state.status).to_lowercase())
        .bind(&intent_graph_bytes)
        .bind(&frontier_bytes)
        .bind(&memory_refs_bytes)
        .bind(&budget_bytes)
        .bind(state.checkpoint_seq as i64)
        .bind(state.created_at as i64)
        .bind(state.last_activity_at as i64)
        .bind(&state.latest_event_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::ConnectionFailed(e.to_string()))?;

        if result.rows_affected() == 0 {
            return Err(StoreError::OptimisticLockConflict {
                expected: expected_version,
                found: 0,
            });
        }

        Ok(true)
    }

    async fn record_side_effect_intent(
        &self,
        intent: &SideEffectIntent,
    ) -> Result<(), StoreError> {
        let payload_bytes = rmp_serde::to_vec(&intent.payload)
            .map_err(|e| StoreError::SerializationError(e.to_string()))?;
        let id_bytes = uuid::Uuid::new_v4().into_bytes().to_vec();
        let idempotency_key = format!("{}:{}", intent.session_id.to_hex(), intent.request_hash);

        sqlx::query(
            "INSERT INTO side_effects (
                id, session_id, event_id, idempotency_key, effect_class,
                status, request_payload, request_hash
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (session_id, idempotency_key) DO NOTHING",
        )
        .bind(&id_bytes)
        .bind(intent.session_id.as_bytes().as_slice())
        .bind(&intent.id)
        .bind(&idempotency_key)
        .bind(format!("{:?}", intent.effect_class).to_uppercase())
        .bind("PENDING")
        .bind(&payload_bytes)
        .bind(&intent.request_hash)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::ConnectionFailed(e.to_string()))?;

        Ok(())
    }

    async fn commit_side_effect(
        &self,
        id: &[u8],
        response_hash: &str,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "UPDATE side_effects SET
                status = 'COMMITTED',
                response_hash = $1,
                committed_at = $2
             WHERE id = $3 AND status = 'PENDING'",
        )
        .bind(response_hash)
        .bind(nexus_core::now_millis() as i64)
        .bind(id)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::ConnectionFailed(e.to_string()))?;

        Ok(())
    }

    async fn acquire_lock(
        &self,
        resource_id: &str,
        session_id: SessionId,
        mode: LockMode,
    ) -> Result<bool, StoreError> {
        let now = nexus_core::now_millis() as i64;
        let mode_str = match mode {
            LockMode::Exclusive => "EXCLUSIVE",
            LockMode::Shared => "SHARED",
        };
        let lease_expiry = now + 3_600_000; // 1 hour default lease

        let result = sqlx::query(
            "INSERT INTO resource_locks (
                resource_id, owner_session, mode, acquired_at, lease_expiry, generation
            ) VALUES ($1, $2, $3, $4, $5, 1)
            ON CONFLICT (resource_id) DO NOTHING",
        )
        .bind(resource_id)
        .bind(session_id.as_bytes().as_slice())
        .bind(mode_str)
        .bind(now)
        .bind(lease_expiry)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::ConnectionFailed(e.to_string()))?;

        Ok(result.rows_affected() > 0)
    }

    async fn release_lock(
        &self,
        resource_id: &str,
        session_id: SessionId,
    ) -> Result<bool, StoreError> {
        let result = sqlx::query(
            "DELETE FROM resource_locks
             WHERE resource_id = $1 AND owner_session = $2",
        )
        .bind(resource_id)
        .bind(session_id.as_bytes().as_slice())
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::ConnectionFailed(e.to_string()))?;

        Ok(result.rows_affected() > 0)
    }

    async fn record_llm_call(&self, call: &LlmCallRecord) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO llm_calls (
                request_id, session_id, event_id, model,
                prompt_hash, response_hash, input_tokens, output_tokens,
                cost_usd_cents, status, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (request_id) DO NOTHING",
        )
        .bind(&call.request_id)
        .bind(call.session_id.as_bytes().as_slice())
        .bind(&call.event_id)
        .bind(&call.model)
        .bind(&call.prompt_hash)
        .bind(&call.response_hash)
        .bind(call.input_tokens)
        .bind(call.output_tokens)
        .bind(call.cost_usd_cents)
        .bind("completed")
        .bind(call.created_at as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::ConnectionFailed(e.to_string()))?;

        Ok(())
    }

    async fn register_artifact(&self, artifact: &ArtifactRef) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO artifact_refs (
                id, kind, uri, blake3, size,
                produced_by_session, produced_by_event, status, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (id) DO NOTHING",
        )
        .bind(artifact.id.as_bytes())
        .bind(format!("{:?}", artifact.kind).to_lowercase())
        .bind(&artifact.uri)
        .bind(&artifact.blake3)
        .bind(artifact.size_bytes as i64)
        .bind(artifact.produced_by_session.as_bytes().as_slice())
        .bind(&artifact.produced_by_event)
        .bind("created")
        .bind(artifact.created_at as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::ConnectionFailed(e.to_string()))?;

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
