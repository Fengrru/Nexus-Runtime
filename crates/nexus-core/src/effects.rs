use crate::types::*;

pub enum RecoveryStrategy {
    Replay,
    Compensate,
    QueryAndConfirm,
}

impl SideEffectClass {
    pub fn recovery_strategy(&self) -> RecoveryStrategy {
        match self {
            SideEffectClass::Pure => RecoveryStrategy::Replay,
            SideEffectClass::Idempotent => RecoveryStrategy::Replay,
            SideEffectClass::Reversible => RecoveryStrategy::Compensate,
            SideEffectClass::Irreversible => RecoveryStrategy::QueryAndConfirm,
        }
    }
}

pub enum RecoveryAction {
    Replay,
    CompensateAndReplay,
    QueryExternal,
    UseCached,
    Retry,
}

pub struct SideEffectGuard {
    effects: Vec<SideEffectRecord>,
}

impl SideEffectGuard {
    pub fn new() -> Self {
        Self { effects: Vec::new() }
    }

    pub fn record_intent(&mut self, intent: SideEffectIntent) -> Result<String, EffectError> {
        let _idempotency_key = format!(
            "{}:{}",
            intent.session_id.to_hex(),
            intent.request_hash
        );

        if let Some(existing) = self
            .effects
            .iter()
            .find(|e| e.intent.request_hash == intent.request_hash && e.intent.session_id == intent.session_id)
        {
            if existing.status == EffectStatus::Pending || existing.status == EffectStatus::Committed {
                return Ok(existing.id.clone());
            }
        }

        let id = format!("se_{}", uuid::Uuid::new_v4().simple());
        let record = SideEffectRecord {
            id: id.clone(),
            session_id: intent.session_id,
            event_id: String::new(),
            intent,
            status: EffectStatus::Pending,
            response_payload: None,
            response_hash: None,
            compensation_data: None,
            committed_at: None,
        };

        self.effects.push(record);
        Ok(id)
    }

    pub fn commit_effect(
        &mut self,
        effect_id: &str,
        response_hash: &str,
    ) -> Result<EffectResult, EffectError> {
        let record = self
            .effects
            .iter_mut()
            .find(|e| e.id == effect_id)
            .ok_or(EffectError::IntentNotFound)?;

        if record.status != EffectStatus::Pending {
            return Err(EffectError::AlreadyProcessed);
        }

        record.status = EffectStatus::Committed;
        record.response_hash = Some(response_hash.to_string());
        record.committed_at = Some(now_millis());

        Ok(EffectResult {
            effect_id: effect_id.to_string(),
            hash: response_hash.to_string(),
            success: true,
        })
    }

    pub fn get_recovery_action(&self, effect_id: &str) -> Result<RecoveryAction, EffectError> {
        let record = self
            .effects
            .iter()
            .find(|e| e.id == effect_id)
            .ok_or(EffectError::IntentNotFound)?;

        match record.status {
            EffectStatus::Pending => match record.intent.effect_class {
                SideEffectClass::Pure | SideEffectClass::Idempotent => Ok(RecoveryAction::Replay),
                SideEffectClass::Reversible => {
                    if record.compensation_data.is_some() {
                        Ok(RecoveryAction::CompensateAndReplay)
                    } else {
                        Ok(RecoveryAction::Replay)
                    }
                }
                SideEffectClass::Irreversible => Ok(RecoveryAction::QueryExternal),
            },
            EffectStatus::Committed => Ok(RecoveryAction::UseCached),
            EffectStatus::Compensated => Ok(RecoveryAction::Replay),
            EffectStatus::Failed => Ok(RecoveryAction::Retry),
        }
    }

    pub fn add_compensation_data(
        &mut self,
        effect_id: &str,
        data: CompensationData,
    ) -> Result<(), EffectError> {
        let record = self
            .effects
            .iter_mut()
            .find(|e| e.id == effect_id)
            .ok_or(EffectError::IntentNotFound)?;

        record.compensation_data = Some(data);
        Ok(())
    }

    pub fn get_effect_by_id(&self, effect_id: &str) -> Option<&SideEffectRecord> {
        self.effects.iter().find(|e| e.id == effect_id)
    }

    pub fn get_effect_by_idempotency(
        &self,
        session_id: SessionId,
        idempotency_key: &str,
    ) -> Option<&SideEffectRecord> {
        self.effects.iter().find(|e| {
            e.intent.session_id == session_id
                && format!("{}:{}", e.intent.session_id.to_hex(), e.intent.request_hash)
                    == idempotency_key
        })
    }
}

impl Default for SideEffectGuard {
    fn default() -> Self {
        Self::new()
    }
}

pub struct EffectResult {
    pub effect_id: String,
    pub hash: String,
    pub success: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum EffectError {
    #[error("Intent not found")]
    IntentNotFound,

    #[error("Already processed")]
    AlreadyProcessed,

    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("Precondition failed: {0}")]
    PreconditionFailed(String),

    #[error("Execution failed: {0}")]
    ExecutionFailed(String),
}

#[derive(Debug, Clone)]
pub struct EffectClassMatrix;

impl EffectClassMatrix {
    pub fn classify(action_type: &str, target: &str) -> SideEffectClass {
        match action_type {
            "read_file" | "grep" | "calculate" | "ls" => SideEffectClass::Pure,
            "upsert" | "replace_text" | "write_file" => SideEffectClass::Idempotent,
            "edit_file" | "create_file" | "patch" => SideEffectClass::Reversible,
            "send_email" | "git_push" | "deploy" | "create_pr" => {
                SideEffectClass::Irreversible
            }
            _ => {
                if target.contains("email") || target.contains("push") {
                    SideEffectClass::Irreversible
                } else if target.contains("file") || target.contains("edit") {
                    SideEffectClass::Reversible
                } else {
                    SideEffectClass::Idempotent
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_side_effect_intent_and_commit() {
        let mut guard = SideEffectGuard::new();
        let intent = SideEffectIntent {
            id: "se_001".into(),
            session_id: SessionId::from_bytes([1u8; 16]),
            task_id: TaskId::from_bytes([2u8; 16]),
            effect_class: SideEffectClass::Idempotent,
            action_type: "write_file".into(),
            target: "/tmp/test.txt".into(),
            payload: vec![],
            request_hash: "abc123".into(),
            preconditions: vec![],
        };

        let id = guard.record_intent(intent).unwrap();
        assert!(!id.is_empty());

        let result = guard.commit_effect(&id, "resp_hash_001").unwrap();
        assert!(result.success);

        let action = guard.get_recovery_action(&id).unwrap();
        assert!(matches!(action, RecoveryAction::UseCached));
    }

    #[test]
    fn test_idempotency_keys() {
        let mut guard = SideEffectGuard::new();
        let intent = SideEffectIntent {
            id: "se_002".into(),
            session_id: SessionId::from_bytes([1u8; 16]),
            task_id: TaskId::from_bytes([2u8; 16]),
            effect_class: SideEffectClass::Idempotent,
            action_type: "write_file".into(),
            target: "/tmp/test.txt".into(),
            payload: vec![],
            request_hash: "duplicate_key".into(),
            preconditions: vec![],
        };

        let id1 = guard.record_intent(intent.clone()).unwrap();
        let id2 = guard.record_intent(intent).unwrap();
        assert_eq!(id1, id2, "Idempotent keys should return same ID");
    }

    #[test]
    fn test_effect_class_matrix() {
        assert_eq!(
            EffectClassMatrix::classify("read_file", "src/main.rs"),
            SideEffectClass::Pure
        );
        assert_eq!(
            EffectClassMatrix::classify("send_email", "admin@example.com"),
            SideEffectClass::Irreversible
        );
        assert_eq!(
            EffectClassMatrix::classify("edit_file", "src/main.rs"),
            SideEffectClass::Reversible
        );
    }
}
