use nexus_core::*;
use std::collections::BTreeMap;

fn main() {
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

    let bytes = serialize_deterministic(&checkpoint).unwrap();
    let path = "fixtures/checkpoint_v0.msgpack";
    std::fs::write(path, &bytes).unwrap();
    println!("Golden fixture written to {} ({} bytes)", path, bytes.len());
}
