#[cfg(test)]
mod tests {
    use crate::*;

    use std::collections::BTreeMap;

    const GOLDEN_CHECKPOINT_V0: &[u8] = include_bytes!("../../../fixtures/checkpoint_v0.msgpack");

    #[test]
    fn golden_checkpoint_serialization_is_deterministic() {
        let checkpoint = Checkpoint {
            checkpoint_id: "cp_golden_001".to_string(),
            session_id: SessionId::from_bytes([
                0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D,
                0x1E, 0x1F,
            ]),
            step_index: 42,
            total_actions: 100,
            replay_actions: vec![
                ReplayAction::ReadFile {
                    path: "/project/src/auth.py".to_string(),
                    expected_hash: "a3f7c2d8e9b104f5a6c3d2e1f0a9b8c7d6e5f4a3".to_string(),
                },
                ReplayAction::EditFile {
                    path: "/project/src/auth.py".to_string(),
                    search: "def authenticate_session".to_string(),
                    replace: "def authenticate_jwt".to_string(),
                    expected_count: 1,
                },
            ],
            artifact_refs: vec![ArtifactRef {
                id: "art_golden_001".into(),
                kind: ArtifactKind::File,
                uri: "vault://artifacts/golden_001".into(),
                blake3: "b7e9a1f3c2d8f4e6a8c0d2e4f6a8b0c2d4e6f8a0".into(),
                size_bytes: 2048,
                produced_by_session: SessionId::from_bytes([0xA0; 16]),
                produced_by_event: "evt_golden".into(),
                created_at: 1717098723000,
            }],
            handle_registry: vec![HandleRecord {
                handle_type: "file_lock".into(),
                reacquire_command: "flock /project/src/auth.py".into(),
                metadata: {
                    let mut m = BTreeMap::new();
                    m.insert("pid".into(), "12345".into());
                    m.insert("fd".into(), "3".into());
                    m
                },
            }],
            determinism_context: DeterminismContext {
                seed: 12345,
                model_version: "claude-3.5-sonnet-20241022".to_string(),
                input_hash: "deadbeefcafe1234deadbeefcafe5678deadbeefcafe9abc".into(),
                checkpoint_format_version: 0,
                worker_type: WorkerType::Python,
            },
            created_at: 1717098723000,
        };

        let actual = serialize_deterministic(&checkpoint).unwrap();

        // Regenerate golden fixture (uncomment to regenerate):
        // std::fs::write("fixtures/checkpoint_v0.msgpack", &actual).unwrap();

        assert_eq!(
            actual, GOLDEN_CHECKPOINT_V0,
            "Checkpoint serialization diverged from golden fixture.\n\
             This indicates a non-deterministic change in serialization format.\n\
             Verify that only BTreeMap, u64, and rmp-serde StructMap are used."
        );
    }

    #[test]
    fn golden_checkpoint_deserialization_round_trip() {
        let cp: Checkpoint = deserialize_deterministic(GOLDEN_CHECKPOINT_V0).unwrap();

        assert_eq!(cp.checkpoint_id, "cp_golden_001");
        assert_eq!(cp.step_index, 42);
        assert_eq!(cp.total_actions, 100);
        assert_eq!(cp.replay_actions.len(), 2);

        match &cp.replay_actions[0] {
            ReplayAction::ReadFile {
                path,
                expected_hash,
            } => {
                assert_eq!(path, "/project/src/auth.py");
                assert_eq!(expected_hash, "a3f7c2d8e9b104f5a6c3d2e1f0a9b8c7d6e5f4a3");
            }
            _ => panic!("expected ReadFile"),
        }

        assert_eq!(cp.handle_registry.len(), 1);
        assert_eq!(cp.handle_registry[0].handle_type, "file_lock");
        assert_eq!(cp.determinism_context.seed, 12345);
        assert_eq!(
            cp.determinism_context.model_version,
            "claude-3.5-sonnet-20241022"
        );

        // Verify determinism: serialize again must produce identical bytes
        let re_serialized = serialize_deterministic(&cp).unwrap();
        assert_eq!(
            re_serialized, GOLDEN_CHECKPOINT_V0,
            "Re-serialization must be byte-identical to golden fixture"
        );
    }

    #[test]
    fn golden_transition_state_is_deterministic() {
        let sid = SessionId::from_bytes([0xAA; 16]);

        // Two independent processes should produce identical state
        let run = || -> NexusState {
            let mut state = NexusState::new(sid, 1717098723000);
            let dag = BTreeMap::new();
            let mut cv = CausalVector::new();

            cv.increment(sid);
            let e1 = NexusEvent::new(
                EventType::IntentReceived {
                    raw_input: "golden test".into(),
                    source: "fixture".into(),
                },
                sid,
                cv.clone(),
                None,
            );
            state = transition(&state, &e1, &dag).unwrap();

            cv.increment(sid);
            let e2 = NexusEvent::new(
                EventType::IntentParsed {
                    intent_graph: IntentGraph::default(),
                },
                sid,
                cv.clone(),
                None,
            );
            state = transition(&state, &e2, &dag).unwrap();

            cv.increment(sid);
            let e3 = NexusEvent::new(
                EventType::PlanCommitted {
                    frontier: Frontier::empty(),
                },
                sid,
                cv.clone(),
                None,
            );
            state = transition(&state, &e3, &dag).unwrap();

            state
        };

        let state1 = run();
        let state2 = run();

        // Deterministic fields must be identical
        assert_eq!(state1.session_id, state2.session_id);
        assert_eq!(state1.status, state2.status);
        assert_eq!(state1.version, state2.version);
        assert_eq!(state1.checkpoint_seq, state2.checkpoint_seq);
        assert_eq!(
            state1.causal_vector.to_canonical(),
            state2.causal_vector.to_canonical(),
            "Causal vector must be deterministic"
        );

        // Non-deterministic fields (UUIDs, timestamps) are allowed to differ
        // but the core state structure must be identical
    }

    #[test]
    fn golden_causal_vector_fixtures() {
        let sid_a = SessionId::from_bytes([0xC1; 16]);
        let sid_b = SessionId::from_bytes([0xC2; 16]);

        // Fixture 1: merge produces deterministic result
        let mut cv1 = CausalVector::new();
        cv1.increment(sid_a);
        cv1.increment(sid_a);
        cv1.increment(sid_a);
        cv1.increment(sid_b);

        let mut cv2 = CausalVector::new();
        cv2.increment(sid_a);
        cv2.increment(sid_a);
        cv2.increment(sid_b);
        cv2.increment(sid_b);

        let mut merged = cv1.clone();
        merged.merge(&cv2);

        assert_eq!(merged.0.get(&sid_a), Some(&3));
        assert_eq!(merged.0.get(&sid_b), Some(&2));

        // Canonical form must be deterministic
        let canonical1 = merged.to_canonical();
        let canonical2 = merged.to_canonical();
        assert_eq!(canonical1, canonical2);

        // Verify happened-before relationship
        assert!(cv2.happened_before(&cv1) || cv1.happened_before(&cv2) || cv1.is_concurrent(&cv2));
    }
}
