-- PostgreSQL schema for Nexus Runtime
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    session_id BYTEA NOT NULL,
    trace_id BYTEA NOT NULL,
    parent_event_id TEXT,
    causal_vector TEXT NOT NULL,
    payload BYTEA NOT NULL,
    payload_hash TEXT NOT NULL,
    event_timestamp BIGINT NOT NULL,
    nonce TEXT NOT NULL,
    integrity_hash TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_session_time ON events(session_id, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_events_trace ON events(trace_id);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_parent ON events(parent_event_id);

CREATE TABLE IF NOT EXISTS sessions (
    session_id BYTEA PRIMARY KEY,
    version BIGINT NOT NULL DEFAULT 1,
    status TEXT NOT NULL,
    intent_graph BYTEA NOT NULL,
    execution_frontier BYTEA NOT NULL,
    memory_refs BYTEA NOT NULL,
    budget BYTEA NOT NULL,
    checkpoint_seq BIGINT NOT NULL DEFAULT 0,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    latest_event_id TEXT NOT NULL REFERENCES events(event_id)
);

CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(status);

CREATE TABLE IF NOT EXISTS side_effects (
    id BYTEA PRIMARY KEY,
    session_id BYTEA NOT NULL,
    event_id TEXT NOT NULL REFERENCES events(event_id),
    idempotency_key TEXT NOT NULL,
    effect_class TEXT NOT NULL CHECK(effect_class IN ('PURE', 'IDEMPOTENT', 'REVERSIBLE', 'IRREVERSIBLE')),
    status TEXT NOT NULL CHECK(status IN ('PENDING', 'COMMITTED', 'COMPENSATED', 'FAILED')),
    request_payload BYTEA NOT NULL,
    request_hash TEXT NOT NULL,
    response_payload BYTEA,
    response_hash TEXT,
    compensation_data BYTEA,
    committed_at BIGINT,
    UNIQUE(session_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_side_effects_session ON side_effects(session_id, status);
CREATE INDEX IF NOT EXISTS idx_side_effects_idempotency ON side_effects(idempotency_key);

CREATE TABLE IF NOT EXISTS resource_locks (
    resource_id TEXT PRIMARY KEY,
    owner_session BYTEA NOT NULL REFERENCES sessions(session_id),
    owner_task BYTEA,
    mode TEXT NOT NULL CHECK(mode IN ('EXCLUSIVE', 'SHARED')),
    acquired_at BIGINT NOT NULL,
    lease_expiry BIGINT,
    generation BIGINT NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_locks_owner ON resource_locks(owner_session);
CREATE INDEX IF NOT EXISTS idx_locks_expiry ON resource_locks(lease_expiry);

CREATE TABLE IF NOT EXISTS llm_calls (
    request_id TEXT PRIMARY KEY,
    session_id BYTEA NOT NULL,
    event_id TEXT NOT NULL REFERENCES events(event_id),
    model TEXT NOT NULL,
    prompt_hash TEXT NOT NULL,
    response_hash TEXT,
    input_tokens BIGINT,
    output_tokens BIGINT,
    cost_usd_cents BIGINT,
    status TEXT NOT NULL,
    created_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_llm_calls_session ON llm_calls(session_id, created_at);
CREATE INDEX IF NOT EXISTS idx_llm_calls_model ON llm_calls(model);

CREATE TABLE IF NOT EXISTS artifact_refs (
    id BYTEA PRIMARY KEY,
    kind TEXT NOT NULL,
    uri TEXT NOT NULL,
    blake3 TEXT NOT NULL,
    size BIGINT NOT NULL,
    produced_by_session BYTEA NOT NULL,
    produced_by_event TEXT NOT NULL REFERENCES events(event_id),
    status TEXT NOT NULL,
    created_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_artifacts_session ON artifact_refs(produced_by_session);
CREATE INDEX IF NOT EXISTS idx_artifacts_blake3 ON artifact_refs(blake3);

CREATE TABLE IF NOT EXISTS memory_graph (
    memory_id TEXT PRIMARY KEY,
    session_origin BYTEA NOT NULL,
    causal_vector TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    content_uri TEXT NOT NULL,
    importance_score BIGINT NOT NULL,
    activation_score BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    last_accessed_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS memory_edges (
    from_memory TEXT NOT NULL,
    to_memory TEXT NOT NULL,
    edge_type TEXT NOT NULL CHECK(edge_type IN ('derives_from', 'contradicts', 'refines', 'generalizes', 'enables', 'caused_by')),
    confidence BIGINT NOT NULL,
    PRIMARY KEY (from_memory, to_memory, edge_type)
);

CREATE INDEX IF NOT EXISTS idx_memory_edges_from ON memory_edges(from_memory);
CREATE INDEX IF NOT EXISTS idx_memory_edges_to ON memory_edges(to_memory);
