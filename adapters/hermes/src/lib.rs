use nexus_core::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct HermesCliAdapter {
    session_id: Option<SessionId>,
    checkpoint_buffer: Vec<CheckpointSnapshot>,
    checkpoint_file: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointSnapshot {
    pub checkpoint_id: String,
    pub session_id: String,
    pub step_index: u64,
    pub actions: Vec<String>,
    pub artifacts: Vec<ArtifactRef>,
    pub timestamp: u64,
}

impl HermesCliAdapter {
    pub fn new() -> Self {
        Self {
            session_id: None,
            checkpoint_buffer: Vec::new(),
            checkpoint_file: None,
        }
    }

    pub fn with_checkpoint_file(mut self, path: &str) -> Self {
        self.checkpoint_file = Some(path.to_string());
        self
    }

    pub fn start_session(&mut self, intent: &str) -> SessionId {
        let session_id = SessionId::new();
        self.session_id = Some(session_id);

        tracing::info!(
            target = "nexus.adapter.hermes",
            session_id = %session_id.to_hex(),
            intent = %intent,
            "Hermes session started"
        );

        session_id
    }

    pub fn record_checkpoint(
        &mut self,
        step_index: u64,
        actions: Vec<String>,
        artifacts: Vec<ArtifactRef>,
    ) {
        let sid = self.session_id.expect("No active session");

        let snapshot = CheckpointSnapshot {
            checkpoint_id: format!("chk_{}_{}", sid.to_hex(), step_index),
            session_id: sid.to_hex(),
            step_index,
            actions,
            artifacts,
            timestamp: now_millis(),
        };

        self.checkpoint_buffer.push(snapshot);
    }

    pub fn to_nexus_event(&self, step_index: u64, actions: Vec<Action>) -> NexusEvent {
        let sid = self.session_id.expect("No active session");

        let mut cv = CausalVector::new();
        for _ in 0..step_index {
            cv.increment(sid);
        }

        NexusEvent::new(
            EventType::WorkerCheckpoint {
                task_id: TaskId::from_bytes([0xAAu8; 16]),
                step_index,
                actions,
                artifacts: vec![],
            },
            sid,
            cv,
            None,
        )
    }

    pub fn to_snapshot(&self, step_index: u64, actions: Vec<Action>) -> CheckpointSnapshot {
        let sid = self.session_id.expect("No active session");
        CheckpointSnapshot {
            checkpoint_id: format!("chk_{}_{}", sid.to_hex(), step_index),
            session_id: sid.to_hex(),
            step_index,
            actions: actions.iter().map(|a| format!("{:?}", a)).collect(),
            artifacts: vec![],
            timestamp: now_millis(),
        }
    }

    pub fn save_to_file(&self) -> Result<(), String> {
        let path = self
            .checkpoint_file
            .as_ref()
            .ok_or("no checkpoint file configured")?;
        let json = serde_json::to_string_pretty(&self.checkpoint_buffer)
            .map_err(|e| format!("serialize: {}", e))?;
        std::fs::write(path, json).map_err(|e| format!("write: {}", e))
    }

    pub fn load_from_file(&mut self, path: &str) -> Result<(), String> {
        let json = std::fs::read_to_string(path).map_err(|e| format!("read: {}", e))?;
        self.checkpoint_buffer =
            serde_json::from_str(&json).map_err(|e| format!("deserialize: {}", e))?;
        if let Some(snapshot) = self.checkpoint_buffer.first() {
            self.session_id = Some(
                SessionId::from_hex(&snapshot.session_id).unwrap_or_else(|_| SessionId::new()),
            );
        }
        Ok(())
    }

    pub fn export_checkpoints(&self) -> Vec<CheckpointSnapshot> {
        self.checkpoint_buffer.clone()
    }

    pub fn import_checkpoints(&mut self, snapshots: Vec<CheckpointSnapshot>) {
        self.checkpoint_buffer = snapshots;
    }

    pub fn get_session_id(&self) -> Option<SessionId> {
        self.session_id
    }

    pub fn checkpoint_count(&self) -> usize {
        self.checkpoint_buffer.len()
    }
}

impl Default for HermesCliAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_hermes_adapter_session_lifecycle() {
        let mut adapter = HermesCliAdapter::new();
        let sid = adapter.start_session("refactor auth");

        assert!(sid.to_hex().len() == 32);

        adapter.record_checkpoint(1, vec!["read auth.py".into()], vec![]);
        adapter.record_checkpoint(2, vec!["edit auth.py".into()], vec![]);

        let checkpoints = adapter.export_checkpoints();
        assert_eq!(checkpoints.len(), 2);
        assert_eq!(checkpoints[0].step_index, 1);
        assert_eq!(checkpoints[1].step_index, 2);
    }

    #[test]
    fn test_cross_session_checkpoint_import() {
        let mut adapter_a = HermesCliAdapter::new();
        adapter_a.start_session("task A");
        adapter_a.record_checkpoint(1, vec!["action 1".into()], vec![]);

        let exported = adapter_a.export_checkpoints();

        let mut adapter_b = HermesCliAdapter::new();
        adapter_b.start_session("task B");
        adapter_b.import_checkpoints(exported);

        let checkpoints = adapter_b.export_checkpoints();
        assert_eq!(checkpoints[0].actions, vec!["action 1"]);
    }

    #[test]
    fn test_hermes_checkpoint_file_persistence() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();

        let mut adapter = HermesCliAdapter::new().with_checkpoint_file(&path);
        adapter.start_session("persist test");
        adapter.record_checkpoint(1, vec!["step 1".into()], vec![]);
        adapter.save_to_file().unwrap();

        let mut adapter2 = HermesCliAdapter::new();
        adapter2.load_from_file(&path).unwrap();
        let checkpoints = adapter2.export_checkpoints();
        assert_eq!(checkpoints.len(), 1);
        assert_eq!(checkpoints[0].step_index, 1);
    }
}
