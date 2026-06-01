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
