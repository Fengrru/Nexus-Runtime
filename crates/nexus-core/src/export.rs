use std::collections::BTreeMap;
use crate::types::*;
use crate::event::*;
use crate::state_machine::*;
use crate::protocol::*;
use serde::{Serialize, Deserialize, Serializer, Deserializer};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionExport {
    pub version: String,
    pub session_id: String,
    pub events: Vec<ExportEvent>,
    pub memory_graph: MemoryGraph,
    #[serde(serialize_with = "ser_cv", deserialize_with = "de_cv")]
    pub causal_vector: CausalVector,
    pub export_hash: String,
    pub exported_at: u64,
}

fn ser_cv<S: Serializer>(cv: &CausalVector, s: S) -> Result<S::Ok, S::Error> {
    cv.to_canonical().serialize(s)
}

fn de_cv<'de, D: Deserializer<'de>>(d: D) -> Result<CausalVector, D::Error> {
    let s = String::deserialize(d)?;
    CausalVector::from_canonical(&s).map_err(serde::de::Error::custom)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportEvent {
    pub event_id: String,
    pub event_type_json: String,
    pub causal_vector_canonical: String,
    pub payload_hash: String,
    pub timestamp: u64,
}

impl SessionExport {
    pub fn from_session(
        events: &[NexusEvent],
        session_id: SessionId,
        memory_graph: MemoryGraph,
        causal_vector: CausalVector,
    ) -> Self {
        let export_events: Vec<ExportEvent> = events
            .iter()
            .map(|e| ExportEvent {
                event_id: e.event_id.clone(),
                event_type_json: serde_json::to_string(&e.event_type).unwrap_or_default(),
                causal_vector_canonical: e.causal_vector.to_canonical(),
                payload_hash: e.payload_hash.clone(),
                timestamp: e.event_timestamp,
            })
            .collect();

        let mut export = Self {
            version: "1.0.0".into(),
            session_id: session_id.to_hex(),
            events: export_events,
            memory_graph,
            causal_vector,
            export_hash: String::new(),
            exported_at: now_millis(),
        };

        let export_bytes = serialize_deterministic(&export).unwrap_or_default();
        export.export_hash = compute_hash(&export_bytes);
        export
    }

    pub fn verify_integrity(&self) -> Result<(), String> {
        if self.version != "1.0.0" {
            return Err(format!("unsupported export version: {}", self.version));
        }

        if self.events.is_empty() {
            return Err("export contains no events".into());
        }

        if !self.causal_vector.is_consistent() {
            return Err("causal vector is inconsistent".into());
        }

        Ok(())
    }

    pub fn verify_export_hash(&self) -> Result<(), String> {
        if self.export_hash.is_empty() {
            return Err("export has no hash to verify".into());
        }

        let mut hashless = self.clone();
        hashless.export_hash = String::new();

        let export_bytes = serialize_deterministic(&hashless).unwrap_or_default();
        let recomputed = compute_hash(&export_bytes);
        if recomputed != self.export_hash {
            return Err(format!(
                "export hash mismatch: expected {}, got {}",
                self.export_hash, recomputed
            ));
        }

        Ok(())
    }

    pub fn replay_into_state(&self) -> Result<NexusState, String> {
        let session_id = SessionId::from_hex(&self.session_id)
            .map_err(|e| format!("invalid session ID: {}", e))?;

        let mut state = NexusState::new(session_id, self.events.first().map(|e| e.timestamp).unwrap_or(0));

        let dag = BTreeMap::new();

        for export_event in &self.events {
            let event_type: EventType = serde_json::from_str(&export_event.event_type_json)
                .map_err(|e| format!("event_type deserialize: {}", e))?;
            let causal_vector = CausalVector::from_canonical(&export_event.causal_vector_canonical)
                .map_err(|e| format!("causal_vector deserialize: {}", e))?;

            let event = NexusEvent {
                event_id: export_event.event_id.clone(),
                event_type,
                session_id,
                trace_id: generate_trace_id(),
                parent_event_id: None,
                causal_vector,
                payload: vec![],
                payload_hash: export_event.payload_hash.clone(),
                event_timestamp: export_event.timestamp,
                nonce: generate_nonce(),
                integrity_hash: String::new(),
            };

            state = transition(&state, &event, &dag)
                .map_err(|e| format!("transition error at {}: {:?}", export_event.event_id, e))?;
        }

        state.memory_graph = self.memory_graph.clone();

        Ok(state)
    }

    pub fn inherit_memories_into(
        &self,
        target: &mut MemoryGraph,
        target_causal_vector: &CausalVector,
    ) -> Result<Vec<String>, String> {
        target.inherit_memories(
            &self.memory_graph,
            SessionId::from_hex(&self.session_id)
                .map_err(|e| format!("invalid session ID: {}", e))?,
            target_causal_vector,
        )
    }

    pub fn to_json(&self) -> Result<String, String> {
        serde_json::to_string_pretty(self)
            .map_err(|e| format!("JSON serialization error: {}", e))
    }

    pub fn from_json(json: &str) -> Result<Self, String> {
        serde_json::from_str(json)
            .map_err(|e| format!("JSON deserialization error: {}", e))
    }

    pub fn to_file(&self, path: &str) -> Result<(), String> {
        let json = self.to_json()?;
        std::fs::write(path, json)
            .map_err(|e| format!("write error: {}", e))
    }

    pub fn from_file(path: &str) -> Result<Self, String> {
        let json = std::fs::read_to_string(path)
            .map_err(|e| format!("read error: {}", e))?;
        Self::from_json(&json)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_export_and_replay() {
        let sid = SessionId::from_bytes([1u8; 16]);
        let mut cv = CausalVector::new();
        cv.increment(sid);

        let events = vec![
            NexusEvent::new(
                EventType::IntentReceived {
                    raw_input: "export test".into(),
                    source: "test".into(),
                },
                sid,
                {
                    let mut c = CausalVector::new();
                    c.increment(sid);
                    c
                },
                None,
            ),
            NexusEvent::new(
                EventType::IntentParsed {
                    intent_graph: IntentGraph::default(),
                },
                sid,
                {
                    let mut c = CausalVector::new();
                    c.increment(sid);
                    c.increment(sid);
                    c
                },
                None,
            ),
        ];

        let export = SessionExport::from_session(
            &events,
            sid,
            MemoryGraph::default(),
            cv.clone(),
        );

        assert!(export.verify_integrity().is_ok());

        let json = export.to_json().unwrap();
        let reimported = SessionExport::from_json(&json).unwrap();
        assert_eq!(export.session_id, reimported.session_id);
        assert_eq!(export.version, reimported.version);
        assert_eq!(export.events.len(), reimported.events.len());

        let state = reimported.replay_into_state().unwrap();
        assert_eq!(state.status, SessionStatus::Planning);
    }

    #[test]
    fn test_cross_session_memory_inheritance() {
        let sid_a = SessionId::from_bytes([1u8; 16]);
        let sid_b = SessionId::from_bytes([2u8; 16]);

        let mut source_memory = MemoryGraph::new();
        source_memory.add_node(MemoryNode {
            id: "mem_001".into(),
            content: MemoryContent::Text {
                text: "JWT auth strategy works well".into(),
            },
            embedding: None,
            causal_context: CausalVector::singleton(sid_a, 3),
            importance: 800,
            activation: 0,
            source_event_id: "evt_001".into(),
            session_lineage: vec![sid_a],
            created_at: now_millis(),
        });

        let export = SessionExport::from_session(
            &[],
            sid_a,
            source_memory,
            CausalVector::singleton(sid_a, 3),
        );

        let mut target_memory = MemoryGraph::new();
        // Target CV must happen-after source CV for inheritance
        let mut target_cv = CausalVector::singleton(sid_a, 5);
        target_cv.increment(sid_b);

        let imported = export
            .inherit_memories_into(&mut target_memory, &target_cv)
            .unwrap();

        assert!(!imported.is_empty());
        assert!(!target_memory.nodes.is_empty());
    }

    #[test]
    fn test_export_file_round_trip() {
        use tempfile::NamedTempFile;

        let sid = SessionId::from_bytes([1u8; 16]);
        let export = SessionExport::from_session(
            &[],
            sid,
            MemoryGraph::default(),
            CausalVector::new(),
        );

        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap();

        export.to_file(path).unwrap();
        let reloaded = SessionExport::from_file(path).unwrap();

        assert_eq!(export.session_id, reloaded.session_id);
        assert_eq!(export.export_hash, reloaded.export_hash);
        assert!(reloaded.verify_export_hash().is_ok());
    }

    #[test]
    fn test_export_hash_tamper_detection() {
        let sid = SessionId::from_bytes([1u8; 16]);
        let mut export = SessionExport::from_session(
            &[],
            sid,
            MemoryGraph::default(),
            CausalVector::new(),
        );

        assert!(export.verify_export_hash().is_ok());

        export.session_id = "tampered".into();
        assert!(export.verify_export_hash().is_err());
    }
}
