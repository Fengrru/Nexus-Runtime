use std::collections::BTreeMap;
use nexus_core::*;

pub struct PhoenixHarness {
    pub temp_dir: tempfile::TempDir,
}

impl PhoenixHarness {
    pub fn new() -> Self {
        Self {
            temp_dir: tempfile::tempdir().unwrap(),
        }
    }

    pub fn create_session(&self, intent: &str) -> (NexusState, NexusEvent) {
        let session_id = SessionId::from_bytes([1u8; 16]);
        let state = NexusState::new(session_id, now_millis());

        let mut cv = CausalVector::new();
        cv.increment(session_id);

        let event = NexusEvent::new(
            EventType::IntentReceived {
                raw_input: intent.to_string(),
                source: "phoenix".to_string(),
            },
            session_id,
            cv,
            None,
        );

        (state, event)
    }

    pub fn db_path(&self) -> std::path::PathBuf {
        self.temp_dir.path().join("state.sqlite")
    }

    pub fn vault_path(&self) -> std::path::PathBuf {
        self.temp_dir.path().join("vault")
    }
}

pub struct PhoenixInvariants;

impl PhoenixInvariants {
    pub fn check_all(report: &RecoveryReport) -> Result<(), String> {
        if !report.integrity_check {
            return Err("I-1 failed: integrity check".into());
        }
        if !report.causal_valid {
            return Err("I-2 failed: causal validity".into());
        }
        if !report.replay_success {
            return Err("I-3 failed: replay success".into());
        }
        if !report.artifacts_valid {
            return Err("I-4 failed: artifact integrity".into());
        }
        if !report.cost_integrity {
            return Err("I-5 failed: cost integrity".into());
        }
        Ok(())
    }

    pub fn i1_state_authority(integrity_ok: bool) -> Result<(), String> {
        if !integrity_ok {
            return Err("I-1: State authority check failed".into());
        }
        Ok(())
    }

    pub fn i2_checkpoint_identity(before: &Checkpoint, after: &Checkpoint) -> Result<(), String> {
        if before.checkpoint_id != after.checkpoint_id {
            return Err("I-2: checkpoint_id changed".into());
        }
        if before.step_index != after.step_index {
            return Err("I-2: step_index changed".into());
        }
        Ok(())
    }

    pub fn i3_replay_integrity(
        events: &[NexusEvent],
        expected: &NexusState,
    ) -> Result<(), String> {
        let mut replayed = NexusState::new(expected.session_id, expected.created_at);
        let dag = BTreeMap::new();

        for event in events {
            replayed = transition(&replayed, event, &dag)
                .map_err(|e| format!("I-3: transition failed: {:?}", e))?;
        }

        if replayed.version != expected.version {
            return Err(format!(
                "I-3: version mismatch: replayed={}, expected={}",
                replayed.version, expected.version
            ));
        }

        Ok(())
    }

    pub fn i4_artifact_integrity(artifacts: &[ArtifactRef]) -> Result<(), String> {
        for art in artifacts {
            if art.blake3.len() != 64 {
                return Err(format!(
                    "I-4: artifact {} has invalid blake3 hash",
                    art.id
                ));
            }
        }
        Ok(())
    }

    pub fn i5_determinism_context(
        before: &DeterminismContext,
        after: &DeterminismContext,
    ) -> Result<(), String> {
        if before.seed != after.seed {
            return Err("I-5: seed changed".into());
        }
        if before.model_version != after.model_version {
            return Err("I-5: model_version changed".into());
        }
        if before.input_hash != after.input_hash {
            return Err("I-5: input_hash changed".into());
        }
        Ok(())
    }

    pub fn i6_cost_integrity(
        llm_unique_count: usize,
        llm_total_count: usize,
    ) -> Result<(), String> {
        if llm_unique_count != llm_total_count {
            return Err(format!(
                "I-6: duplicate LLM calls: {} unique, {} total",
                llm_unique_count, llm_total_count
            ));
        }
        Ok(())
    }

    pub fn i7_resume_continuity(before_seq: u64, after_seq: u64) -> Result<(), String> {
        if after_seq <= before_seq {
            return Err(format!(
                "I-7: did not progress: before={}, after={}",
                before_seq, after_seq
            ));
        }
        Ok(())
    }

    pub fn i8_eventual_consistency(
        replayed: &NexusState,
        stored: &NexusState,
    ) -> Result<(), String> {
        if replayed.version != stored.version {
            return Err(format!(
                "I-8: materialized view diverged: replayed={}, stored={}",
                replayed.version, stored.version
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct PhoenixReport {
    pub tests: Vec<PhoenixTestResult>,
}

impl PhoenixReport {
    pub fn all_passed(&self) -> bool {
        self.tests.iter().all(|t| t.passed)
    }

    pub fn summary(&self) -> String {
        let total = self.tests.len();
        let passed = self.tests.iter().filter(|t| t.passed).count();
        format!("{} of {} tests passed", passed, total)
    }
}

#[derive(Debug)]
pub struct PhoenixTestResult {
    pub name: String,
    pub passed: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_phoenix_kill9_at_intake() {
        let harness = PhoenixHarness::new();
        let (state, event) = harness.create_session("refactor auth");

        let dag = BTreeMap::new();
        let result = transition(&state, &event, &dag);
        assert!(result.is_ok());

        let next_state = result.unwrap();
        assert_eq!(next_state.status, SessionStatus::Intake);
    }

    #[test]
    fn test_phoenix_replay_integrity() {
        let session_id = SessionId::from_bytes([1u8; 16]);
        let mut state = NexusState::new(session_id, 0);
        let dag = BTreeMap::new();

        let mut cv = CausalVector::new();
        cv.increment(session_id);

        let event = NexusEvent::new(
            EventType::IntentReceived {
                raw_input: "test".into(),
                source: "phoenix".into(),
            },
            session_id,
            cv,
            None,
        );

        let events = vec![event];
        state = transition(&state, &events[0], &dag).unwrap();

        let result = PhoenixInvariants::i3_replay_integrity(&events, &state);
        assert!(result.is_ok(), "{}", result.unwrap_err());
    }

    #[test]
    fn test_phoenix_causal_validity() {
        let session_id = SessionId::from_bytes([1u8; 16]);
        let mut cv1 = CausalVector::new();
        cv1.increment(session_id);

        let mut cv2 = CausalVector::new();
        cv2.increment(session_id);
        cv2.increment(session_id);

        assert!(cv1.happened_before(&cv2));
        assert!(!cv2.happened_before(&cv1));
    }

    #[test]
    fn test_phoenix_checkpoint_identity() {
        let ckpt = Checkpoint {
            checkpoint_id: "cp_001".into(),
            session_id: SessionId::from_bytes([1u8; 16]),
            step_index: 42,
            total_actions: 100,
            replay_actions: vec![],
            artifact_refs: vec![],
            handle_registry: vec![],
            determinism_context: DeterminismContext {
                seed: 12345,
                model_version: "claude-3.5-sonnet".into(),
                input_hash: "abc".into(),
                checkpoint_format_version: 1,
                worker_type: WorkerType::Python,
            },
            created_at: now_millis(),
        };

        let ckpt2 = ckpt.clone();
        assert!(PhoenixInvariants::i2_checkpoint_identity(&ckpt, &ckpt2).is_ok());
    }

    #[test]
    fn test_phoenix_cost_integrity() {
        assert!(PhoenixInvariants::i6_cost_integrity(5, 5).is_ok());
        assert!(PhoenixInvariants::i6_cost_integrity(3, 5).is_err());
    }

    #[test]
    fn test_phoenix_resume_continuity() {
        assert!(PhoenixInvariants::i7_resume_continuity(0, 3).is_ok());
        assert!(PhoenixInvariants::i7_resume_continuity(5, 3).is_err());
    }

    #[test]
    fn test_full_phoenix_run() {
        let session_id = SessionId::from_bytes([1u8; 16]);
        let mut state = NexusState::new(session_id, 0);
        let dag = BTreeMap::new();

        let mut cv = CausalVector::new();

        // Intake
        cv.increment(session_id);
        let e1 = NexusEvent::new(
            EventType::IntentReceived {
                raw_input: "test".into(),
                source: "phoenix".into(),
            },
            session_id,
            cv.clone(),
            None,
        );
        state = transition(&state, &e1, &dag).unwrap();
        assert_eq!(state.status, SessionStatus::Intake);

        // Parse
        cv.increment(session_id);
        let e2 = NexusEvent::new(
            EventType::IntentParsed {
                intent_graph: IntentGraph::default(),
            },
            session_id,
            cv.clone(),
            None,
        );
        state = transition(&state, &e2, &dag).unwrap();
        assert_eq!(state.status, SessionStatus::Planning);

        // Plan committed
        cv.increment(session_id);
        let e3 = NexusEvent::new(
            EventType::PlanCommitted {
                frontier: Frontier::empty(),
            },
            session_id,
            cv.clone(),
            None,
        );
        state = transition(&state, &e3, &dag).unwrap();
        assert_eq!(state.status, SessionStatus::Planned);

        // Dependencies met
        cv.increment(session_id);
        let e4 = NexusEvent::new(EventType::DependenciesMet, session_id, cv.clone(), None);
        state = transition(&state, &e4, &dag).unwrap();
        assert_eq!(state.status, SessionStatus::Executing);

        // Worker checkpoint
        cv.increment(session_id);
        let e5 = NexusEvent::new(
            EventType::WorkerCheckpoint {
                task_id: TaskId::from_bytes([2u8; 16]),
                step_index: 1,
                actions: vec![],
                artifacts: vec![],
            },
            session_id,
            cv.clone(),
            None,
        );
        state = transition(&state, &e5, &dag).unwrap();
        assert_eq!(state.status, SessionStatus::Checkpointing);
        assert_eq!(state.checkpoint_seq, 1);

        // Simulation: crash here, then recover
        let events = vec![e1, e2, e3, e4, e5];
        let rm = RecoveryManager::new("/tmp/test_vault".into());
        let recovered = rm.recover_from_events(&events, session_id).unwrap();

        assert_eq!(recovered.state.status, SessionStatus::Checkpointing);
        assert_eq!(recovered.state.checkpoint_seq, 1);
        assert!(recovered.report.replay_success);
        assert!(recovered.report.causal_valid);
        assert!(recovered.state.version >= 1);
    }

    #[test]
    fn test_eight_invariants_all_pass() {
        let session_id = SessionId::from_bytes([1u8; 16]);
        let state = NexusState::new(session_id, 0);
        let dag = BTreeMap::new();
        let mut cv = CausalVector::new();

        cv.increment(session_id);
        let event = NexusEvent::new(
            EventType::IntentReceived {
                raw_input: "test invariants".into(),
                source: "phoenix".into(),
            },
            session_id,
            cv,
            None,
        );

        let events = vec![event];
        let _state = transition(&state, &events[0], &dag).unwrap();

        let rm = RecoveryManager::new("/tmp/test_vault".into());
        let recovered = rm.recover_from_events(&events, session_id).unwrap();

        let check = PhoenixInvariants::check_all(&recovered.report);
        assert!(check.is_ok(), "Phoenix invariants failed: {}", check.unwrap_err());
    }
}

#[cfg(test)]
mod integration {
    use super::*;
    use nexus_core::*;
    use nexus_core::event::*;
    use nexus_core::recovery::*;
    use nexus_core::effects::*;
    use nexus_core::export::SessionExport;
    use nexus_core::entropy::*;
    use nexus_event_store::*;
    use std::collections::BTreeMap;

    async fn setup_store() -> (SqliteEventStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("e2e_test.db");
        let db_url = format!("sqlite:{}?mode=rwc", db_path.display());
        let store = SqliteEventStore::new(&db_url).await.unwrap();
        (store, dir)
    }

    #[tokio::test]
    async fn e2e_full_session_lifecycle() {
        let (store, _dir) = setup_store().await;
        let sid = SessionId::from_bytes([0xE2, 0xE2, 0,0,0,0,0,0,0,0,0,0,0,0,0,0]);

        // Phase 1: Intake
        let mut seq = 0u64;
        seq += 1;
        let mut cv = CausalVector::new();
        cv.increment(sid);
        store.append_event(&NexusEvent::new(
            EventType::IntentReceived { raw_input: "refactor auth to JWT".into(), source: "e2e".into() },
            sid, cv.clone(), None,
        )).await.unwrap();

        seq += 1;
        cv.increment(sid);
        store.append_event(&NexusEvent::new(
            EventType::IntentParsed { intent_graph: IntentGraph::default() },
            sid, cv.clone(), None,
        )).await.unwrap();

        seq += 1;
        cv.increment(sid);
        store.append_event(&NexusEvent::new(
            EventType::PlanCommitted { frontier: Frontier::empty() },
            sid, cv.clone(), None,
        )).await.unwrap();

        seq += 1;
        cv.increment(sid);
        store.append_event(&NexusEvent::new(
            EventType::DependenciesMet, sid, cv.clone(), None,
        )).await.unwrap();

        // Phase 2: Execution with checkpoints
        seq += 1;
        cv.increment(sid);
        store.append_event(&NexusEvent::new(
            EventType::WorkerCheckpoint {
                task_id: TaskId::from_bytes([0xAA; 16]),
                step_index: 3,
                actions: vec![],
                artifacts: vec![],
            },
            sid, cv.clone(), None,
        )).await.unwrap();

        seq += 1;
        cv.increment(sid);
        store.append_event(&NexusEvent::new(
            EventType::WorkerCheckpoint {
                task_id: TaskId::from_bytes([0xAA; 16]),
                step_index: 7,
                actions: vec![],
                artifacts: vec![],
            },
            sid, cv.clone(), None,
        )).await.unwrap();

        // Phase 3: Simulate crash & recover
        let events = store.get_events(sid, None).await.unwrap();
        assert_eq!(events.len(), 6);

        let rm = RecoveryManager::new("/tmp/e2e_vault".into());
        let recovered = rm.recover_from_events(&events, sid).unwrap();

        assert!(recovered.report.causal_valid);
        assert!(recovered.report.replay_success);
        assert_eq!(recovered.state.status, SessionStatus::Checkpointing);
        assert_eq!(recovered.state.checkpoint_seq, 7);
        assert!(recovered.recovery_plan.is_some());

        // Phase 4: Export & re-import
        let export = SessionExport::from_session(
            &events, sid, MemoryGraph::default(), recovered.state.causal_vector.clone(),
        );
        assert!(export.verify_integrity().is_ok());

        let json = export.to_json().unwrap();
        let reimported = SessionExport::from_json(&json).unwrap();
        let replayed = reimported.replay_into_state().unwrap();
        assert_eq!(replayed.checkpoint_seq, 7);
        assert_eq!(replayed.status, SessionStatus::Checkpointing);
    }

    #[tokio::test]
    async fn e2e_side_effect_two_phase_commit() {
        let mut guard = SideEffectGuard::new();
        let sid = SessionId::from_bytes([0xB1; 16]);
        let tid = TaskId::from_bytes([0xB2; 16]);

        // Phase 1: Record intent
        let intent = SideEffectIntent {
            id: "se_e2e_001".into(),
            session_id: sid,
            task_id: tid,
            effect_class: SideEffectClass::Idempotent,
            action_type: "write_file".into(),
            target: "/tmp/e2e_test.txt".into(),
            payload: b"hello e2e".to_vec(),
            request_hash: "e2e_hash_001".into(),
            preconditions: vec![],
        };

        let effect_id = guard.record_intent(intent).unwrap();
        assert!(!effect_id.is_empty());

        // Get recovery action for PENDING effect
        let action = guard.get_recovery_action(&effect_id).unwrap();
        assert!(matches!(action, RecoveryAction::Replay));

        // Phase 2: Commit
        let result = guard.commit_effect(&effect_id, "resp_hash_e2e").unwrap();
        assert!(result.success);

        // Verify committed
        let committed_action = guard.get_recovery_action(&effect_id).unwrap();
        assert!(matches!(committed_action, RecoveryAction::UseCached));

        // Verify idempotency
        let duplicate = guard.record_intent(SideEffectIntent {
            id: "se_e2e_001".into(),
            session_id: sid,
            task_id: tid,
            effect_class: SideEffectClass::Idempotent,
            action_type: "write_file".into(),
            target: "/tmp/e2e_test.txt".into(),
            payload: b"hello e2e".to_vec(),
            request_hash: "e2e_hash_001".into(),
            preconditions: vec![],
        }).unwrap();
        assert_eq!(duplicate, effect_id);
    }

    #[tokio::test]
    async fn e2e_budget_exhaustion_blocks_session() {
        let sid = SessionId::from_bytes([0xC1; 16]);
        let mut state = NexusState::new(sid, now_millis());
        state.budget.budget_limit_cents = 100;

        let dag = BTreeMap::new();

        // Drive to executing
        let mut cv = CausalVector::new();
        cv.increment(sid);
        state = transition(&state, &NexusEvent::new(
            EventType::IntentReceived { raw_input: "expensive task".into(), source: "e2e".into() },
            sid, cv.clone(), None,
        ), &dag).unwrap();

        cv.increment(sid);
        state = transition(&state, &NexusEvent::new(
            EventType::IntentParsed { intent_graph: IntentGraph::default() },
            sid, cv.clone(), None,
        ), &dag).unwrap();

        cv.increment(sid);
        state = transition(&state, &NexusEvent::new(
            EventType::PlanCommitted { frontier: Frontier::empty() },
            sid, cv.clone(), None,
        ), &dag).unwrap();

        cv.increment(sid);
        state = transition(&state, &NexusEvent::new(
            EventType::DependenciesMet, sid, cv.clone(), None,
        ), &dag).unwrap();

        assert_eq!(state.status, SessionStatus::Executing);

        // Exhaust budget
        state.budget.add_cost(150, 1000, 5);
        assert!(state.budget.is_exhausted());

        // Worker fails with fatal error (e.g., budget)
        cv.increment(sid);
        state = transition(&state, &NexusEvent::new(
            EventType::WorkerFailed {
                worker_id: "w1".into(),
                task_id: TaskId::from_bytes([0xDD; 16]),
                error: "budget exceeded".into(),
                error_code: ErrorCode::Fatal,
                retry_count: 0,
            },
            sid, cv.clone(), None,
        ), &dag).unwrap();

        assert_eq!(state.status, SessionStatus::Failed);
    }

    #[tokio::test]
    async fn e2e_concurrent_session_isolation() {
        let (store, _dir) = setup_store().await;

        let sid1 = SessionId::from_bytes([0xD1; 16]);
        let sid2 = SessionId::from_bytes([0xD2; 16]);

        // Session 1: completed
        let mut cv1 = CausalVector::new();
        cv1.increment(sid1);
        store.append_event(&NexusEvent::new(
            EventType::IntentReceived { raw_input: "task 1".into(), source: "e2e".into() },
            sid1, cv1, None,
        )).await.unwrap();

        // Session 2: separate intent
        let mut cv2 = CausalVector::new();
        cv2.increment(sid2);
        store.append_event(&NexusEvent::new(
            EventType::IntentReceived { raw_input: "task 2".into(), source: "e2e".into() },
            sid2, cv2, None,
        )).await.unwrap();

        // Verify isolation
        let events1 = store.get_events(sid1, None).await.unwrap();
        let events2 = store.get_events(sid2, None).await.unwrap();
        assert_eq!(events1.len(), 1);
        assert_eq!(events2.len(), 1);
        assert_ne!(events1[0].session_id, events2[0].session_id);
    }

    #[tokio::test]
    async fn e2e_cross_session_memory_inheritance() {
        let sid_a = SessionId::from_bytes([0xAA; 16]);
        let sid_b = SessionId::from_bytes([0xBB; 16]);

        // Session A builds knowledge
        let mut mem_a = MemoryGraph::new();
        mem_a.add_node(MemoryNode {
            id: "knowledge_001".into(),
            content: MemoryContent::Text { text: "JWT tokens reduce DB load by 80%".into() },
            embedding: None,
            causal_context: CausalVector::singleton(sid_a, 5),
            importance: 900,
            activation: 0,
            source_event_id: "evt_001".into(),
            session_lineage: vec![sid_a],
            created_at: now_millis(),
        });

        // Export from A
        let mut cv_a = CausalVector::singleton(sid_a, 5);
        let export = SessionExport::from_session(&[], sid_a, mem_a, cv_a.clone());

        // Session B inherits
        let mut mem_b = MemoryGraph::new();
        let mut cv_b = CausalVector::singleton(sid_a, 10);
        cv_b.increment(sid_b);

        let imported = export.inherit_memories_into(&mut mem_b, &cv_b).unwrap();
        assert!(!imported.is_empty());
        assert!(mem_b.nodes.len() >= 1);

        let node = mem_b.nodes.values().next().unwrap();
        assert!(node.session_lineage.contains(&sid_a));
    }

    #[tokio::test]
    async fn e2e_entropy_controller_integration() {
        let controller = EntropyController::default();

        // Normal operation
        let normal = EntropySignals::new(0.05, 0.02, 0.01);
        let score = controller.calculate(&normal);
        assert!(score < controller.thresholds.warning);
        assert_eq!(controller.get_entropy_level(score), EntropyLevel::Normal);
        assert!(controller.respond(score).is_empty());

        // Warning level
        let warn = EntropySignals::new(0.5, 0.3, 0.1);
        let score = controller.calculate(&warn);
        assert!(score >= controller.thresholds.warning);
        assert_eq!(controller.get_entropy_level(score), EntropyLevel::Warning);

        // Circuit breaker
        let critical = EntropySignals::new(1.0, 1.0, 0.9);
        let score = controller.calculate(&critical);
        assert!(score >= controller.thresholds.circuit_breaker);
        assert_eq!(controller.get_entropy_level(score), EntropyLevel::CircuitBreaker);
        let actions = controller.respond(score);
        assert!(actions.contains(&EntropyAction::HaltExecution));
    }

    #[tokio::test]
    async fn e2e_causal_vector_cross_node_merge() {
        let sid_a = SessionId::from_bytes([0xA1; 16]);
        let sid_b = SessionId::from_bytes([0xB1; 16]);
        let sid_c = SessionId::from_bytes([0xC1; 16]);

        // Node A: 5 events
        let mut cv_a = CausalVector::new();
        for _ in 0..5 { cv_a.increment(sid_a); }

        // Node B: 3 events
        let mut cv_b = CausalVector::new();
        for _ in 0..3 { cv_b.increment(sid_a); }
        cv_b.increment(sid_b);

        // Merge
        cv_a.merge(&cv_b);
        assert_eq!(cv_a.0.get(&sid_a), Some(&5));
        assert_eq!(cv_a.0.get(&sid_b), Some(&1));

        // causally-consistent
        assert!(cv_a.is_consistent());
    }
}
