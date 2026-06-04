use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::collections::BTreeMap;
use nexus_core::*;

fn bench_transition(c: &mut Criterion) {
    c.bench_function("transition_intent_received", |b| {
        let sid = SessionId::from_bytes([1u8; 16]);
        let state = NexusState::new(sid, 0);
        let mut cv = CausalVector::new();
        cv.increment(sid);
        let event = NexusEvent::new(
            EventType::IntentReceived {
                raw_input: "benchmark intent".into(),
                source: "bench".into(),
            },
            sid,
            cv,
            None,
        );
        let dag = BTreeMap::new();

        b.iter(|| {
            transition(black_box(&state), black_box(&event), black_box(&dag)).unwrap()
        });
    });

    c.bench_function("transition_full_lifecycle_10_events", |b| {
        let sid = SessionId::from_bytes([2u8; 16]);
        let dag = BTreeMap::new();

        b.iter(|| {
            let mut state = NexusState::new(sid, 0);
            let mut cv = CausalVector::new();

            for step in 0..10 {
                cv.increment(sid);
                let event_type = match step % 10 {
                    0 => EventType::IntentReceived {
                        raw_input: "bench".into(),
                        source: "bench".into(),
                    },
                    1 => EventType::IntentParsed {
                        intent_graph: IntentGraph::default(),
                    },
                    2 => EventType::PlanCommitted {
                        frontier: Frontier::empty(),
                    },
                    3 => EventType::DependenciesMet,
                    _ => EventType::WorkerCheckpoint {
                        task_id: TaskId::from_bytes([3u8; 16]),
                        step_index: step as u64 + 1,
                        actions: vec![],
                        artifacts: vec![],
                    },
                };

                let event = NexusEvent::new(event_type, sid, cv.clone(), None);
                state = transition(&state, &event, &dag).unwrap();
            }
            state
        });
    });
}

fn bench_causal_vector(c: &mut Criterion) {
    c.bench_function("causal_vector_merge_1000", |b| {
        let mut base = CausalVector::new();
        for i in 0..1000 {
            base.increment(SessionId::from_bytes([(i % 255) as u8; 16]));
        }
        let other = base.clone();

        b.iter(|| {
            let mut merged = base.clone();
            merged.merge(black_box(&other));
            merged
        });
    });

    c.bench_function("causal_vector_happened_before", |b| {
        let mut before = CausalVector::new();
        let sid = SessionId::from_bytes([1u8; 16]);
        before.increment(sid);
        before.increment(sid);

        let mut after = CausalVector::new();
        after.increment(sid);
        after.increment(sid);
        after.increment(sid);
        after.increment(sid);

        b.iter(|| {
            before.happened_before(black_box(&after))
        });
    });

    c.bench_function("causal_vector_to_canonical", |b| {
        let sid = SessionId::from_bytes([1u8; 16]);
        let mut cv = CausalVector::new();
        for _ in 0..100 {
            cv.increment(sid);
        }

        b.iter(|| {
            cv.to_canonical()
        });
    });

    c.bench_function("causal_vector_from_canonical", |b| {
        let sid = SessionId::from_bytes([1u8; 16]);
        let mut cv = CausalVector::new();
        for _ in 0..100 {
            cv.increment(sid);
        }
        let canonical = cv.to_canonical();

        b.iter(|| {
            CausalVector::from_canonical(black_box(&canonical)).unwrap()
        });
    });
}

fn bench_serialization(c: &mut Criterion) {
    c.bench_function("serialize_checkpoint_msgpack", |b| {
        let cp = Checkpoint {
            checkpoint_id: "cp_bench_001".into(),
            session_id: SessionId::from_bytes([1u8; 16]),
            step_index: 42,
            total_actions: 100,
            replay_actions: vec![],
            artifact_refs: vec![],
            handle_registry: vec![],
            determinism_context: DeterminismContext {
                seed: 12345,
                model_version: "claude-3.5-sonnet".into(),
                input_hash: "abcdef".into(),
                checkpoint_format_version: 1,
                worker_type: WorkerType::Python,
            },
            created_at: now_millis(),
        };

        b.iter(|| {
            serialize_deterministic(black_box(&cp))
        });
    });

    c.bench_function("hash_blake3_1kb", |b| {
        let data = vec![0x42u8; 1024];
        b.iter(|| {
            compute_hash(black_box(&data))
        });
    });

    c.bench_function("hash_blake3_64kb", |b| {
        let data = vec![0x42u8; 65536];
        b.iter(|| {
            compute_hash(black_box(&data))
        });
    });
}

fn bench_recovery(c: &mut Criterion) {
    c.bench_function("recover_100_events", |b| {
        let sid = SessionId::from_bytes([1u8; 16]);
        let mut events = Vec::new();
        let mut cv = CausalVector::new();

        for i in 0..100 {
            cv.increment(sid);
            events.push(NexusEvent::new(
                EventType::WorkerCheckpoint {
                    task_id: TaskId::from_bytes([2u8; 16]),
                    step_index: i + 1,
                    actions: vec![],
                    artifacts: vec![],
                },
                sid,
                cv.clone(),
                None,
            ));
        }

        let rm = RecoveryManager::new("/tmp/bench_vault".into());

        b.iter(|| {
            rm.recover_from_events(black_box(&events), sid).unwrap()
        });
    });

    c.bench_function("recover_1000_events", |b| {
        let sid = SessionId::from_bytes([1u8; 16]);
        let mut events = Vec::new();
        let mut cv = CausalVector::new();

        for i in 0..1000 {
            cv.increment(sid);
            events.push(NexusEvent::new(
                EventType::WorkerCheckpoint {
                    task_id: TaskId::from_bytes([2u8; 16]),
                    step_index: i + 1,
                    actions: vec![],
                    artifacts: vec![],
                },
                sid,
                cv.clone(),
                None,
            ));
        }

        let rm = RecoveryManager::new("/tmp/bench_vault".into());

        b.iter(|| {
            rm.recover_from_events(black_box(&events), sid).unwrap()
        });
    });
}

fn bench_memory_graph(c: &mut Criterion) {
    c.bench_function("memory_graph_query_causal", |b| {
        let mut graph = MemoryGraph::new();
        for i in 0..100 {
            graph.add_node(MemoryNode {
                id: format!("mem_{:04}", i),
                content: MemoryContent::Text {
                    text: format!("memory node {}", i),
                },
                embedding: None,
                causal_context: CausalVector::singleton(
                    SessionId::from_bytes([1u8; 16]),
                    i,
                ),
                importance: 500,
                activation: 0,
                source_event_id: format!("evt_{}", i),
                session_lineage: vec![],
                created_at: now_millis(),
            });
        }
        for i in 0..99 {
            graph.add_edge(MemoryEdge {
                from: format!("mem_{:04}", i),
                to: format!("mem_{:04}", i + 1),
                edge_type: MemoryEdgeType::DerivesFrom,
                confidence: 7000,
                created_at: now_millis(),
            });
        }

        b.iter(|| {
            graph.query_causal("mem_0000", None, 3)
        });
    });
}

criterion_group!(
    benches,
    bench_transition,
    bench_causal_vector,
    bench_serialization,
    bench_recovery,
    bench_memory_graph,
);
criterion_main!(benches);
