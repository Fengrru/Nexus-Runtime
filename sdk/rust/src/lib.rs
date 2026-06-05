pub use nexus_core::*;
pub use nexus_event_store::*;
pub use nexus_rpc::*;
pub use nexus_scheduler::*;
pub use nexus_security::*;

use std::path::PathBuf;

pub struct RuntimeBuilder {
    mode: DeploymentMode,
    db_path: Option<PathBuf>,
    vault_path: Option<PathBuf>,
    max_workers: usize,
    default_model: String,
}

pub enum DeploymentMode {
    Lite,
    Pro,
    Enterprise,
}

impl Default for RuntimeBuilder {
    fn default() -> Self {
        Self {
            mode: DeploymentMode::Lite,
            db_path: None,
            vault_path: None,
            max_workers: 4,
            default_model: "claude-3.5-sonnet".into(),
        }
    }
}

impl RuntimeBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn mode(mut self, mode: DeploymentMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn db_path(mut self, path: PathBuf) -> Self {
        self.db_path = Some(path);
        self
    }

    pub fn vault_path(mut self, path: PathBuf) -> Self {
        self.vault_path = Some(path);
        self
    }

    pub fn max_workers(mut self, n: usize) -> Self {
        self.max_workers = n;
        self
    }

    pub fn model(mut self, model: impl Into<String>) -> Self {
        self.default_model = model.into();
        self
    }

    pub fn build(self) -> Runtime {
        let db_path = self.db_path.unwrap_or_else(|| {
            let home = std::env::var("HOME")
                .or_else(|_| std::env::var("USERPROFILE"))
                .unwrap_or_else(|_| ".".into());
            PathBuf::from(home).join(".nexus").join("events.db")
        });

        let vault_path = self.vault_path.unwrap_or_else(|| {
            let home = std::env::var("HOME")
                .or_else(|_| std::env::var("USERPROFILE"))
                .unwrap_or_else(|_| ".".into());
            PathBuf::from(home).join(".nexus").join("vault")
        });

        Runtime {
            mode: self.mode,
            db_path,
            vault_path,
            max_workers: self.max_workers,
            default_model: self.default_model,
        }
    }
}

pub struct Runtime {
    pub mode: DeploymentMode,
    pub db_path: PathBuf,
    pub vault_path: PathBuf,
    pub max_workers: usize,
    pub default_model: String,
}

impl Runtime {
    pub fn builder() -> RuntimeBuilder {
        RuntimeBuilder::default()
    }

    pub fn lite() -> RuntimeBuilder {
        RuntimeBuilder::new().mode(DeploymentMode::Lite)
    }

    pub fn pro() -> RuntimeBuilder {
        RuntimeBuilder::new().mode(DeploymentMode::Pro)
    }

    pub async fn create_session(
        &self,
        intent: &str,
        model: Option<&str>,
        budget_usd: f64,
    ) -> SessionHandle {
        let session_id = SessionId::new();
        let budget_cents = (budget_usd * 100.0) as u64;

        let mut cv = CausalVector::new();
        cv.increment(session_id);

        let event = NexusEvent::new(
            EventType::IntentReceived {
                raw_input: intent.to_string(),
                source: "rust-sdk".to_string(),
            },
            session_id,
            cv,
            None,
        );

        SessionHandle {
            session_id,
            intent: intent.to_string(),
            model: model.unwrap_or(&self.default_model).to_string(),
            budget_cents,
            checkpoint_seq: 0,
            status: SessionStatus::Created,
            event,
        }
    }

    pub async fn resume_session(
        &self,
        session_id: SessionId,
        store: &dyn EventStore,
    ) -> Result<SessionHandle, String> {
        let events = store
            .get_events(session_id, None)
            .await
            .map_err(|e| format!("failed to load events: {}", e))?;

        if events.is_empty() {
            return Err("no events found".into());
        }

        let last_event = events.last().unwrap();

        Ok(SessionHandle {
            session_id,
            intent: "recovered".into(),
            model: self.default_model.clone(),
            budget_cents: 500,
            checkpoint_seq: 0,
            status: SessionStatus::Executing,
            event: last_event.clone(),
        })
    }
}

pub struct SessionHandle {
    pub session_id: SessionId,
    pub intent: String,
    pub model: String,
    pub budget_cents: u64,
    pub checkpoint_seq: u64,
    pub status: SessionStatus,
    pub event: NexusEvent,
}

impl SessionHandle {
    pub fn id(&self) -> SessionId {
        self.session_id
    }

    pub fn id_hex(&self) -> String {
        self.session_id.to_hex()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runtime_builder_defaults() {
        let rt = Runtime::builder().build();
        assert!(matches!(rt.mode, DeploymentMode::Lite));
        assert_eq!(rt.max_workers, 4);
        assert_eq!(rt.default_model, "claude-3.5-sonnet");
    }

    #[test]
    fn test_runtime_builder_custom() {
        let rt = Runtime::builder()
            .mode(DeploymentMode::Pro)
            .max_workers(8)
            .model("gpt-4o")
            .build();
        assert!(matches!(rt.mode, DeploymentMode::Pro));
        assert_eq!(rt.max_workers, 8);
        assert_eq!(rt.default_model, "gpt-4o");
    }

    #[tokio::test]
    async fn test_create_session() {
        let rt = Runtime::builder().build();
        let session = rt.create_session("test intent", None, 5.00).await;
        assert_eq!(session.intent, "test intent");
        assert_eq!(session.budget_cents, 500);
        assert!(matches!(session.status, SessionStatus::Created));
    }
}
