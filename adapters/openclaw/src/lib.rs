use nexus_core::*;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone)]
pub struct OpenClawGatewayAdapter {
    session_id: Option<SessionId>,
    gateway_url: String,
}

impl OpenClawGatewayAdapter {
    pub fn new(gateway_url: String) -> Self {
        Self {
            session_id: None,
            gateway_url,
        }
    }

    pub fn initialize_session(&mut self, intent: &str) -> (SessionId, NexusState) {
        let session_id = SessionId::new();
        let state = NexusState::new(session_id, now_millis());
        self.session_id = Some(session_id);

        tracing::info!(
            target = "nexus.adapter.openclaw",
            session_id = %session_id.to_hex(),
            intent = %intent,
            "OpenClaw session initialized"
        );

        (session_id, state)
    }

    pub fn bridge_intent(
        &self,
        raw_input: &str,
        source_channel: &str,
    ) -> NexusEvent {
        let sid = self.session_id.expect("Session not initialized");

        let mut cv = CausalVector::new();
        cv.increment(sid);

        NexusEvent::new(
            EventType::IntentReceived {
                raw_input: raw_input.to_string(),
                source: format!("openclaw:{}", source_channel),
            },
            sid,
            cv,
            None,
        )
    }

    pub fn bridge_result(
        &self,
        output: &str,
        artifacts: Vec<ArtifactRef>,
    ) -> OpenClawResponse {
        OpenClawResponse {
            session_id: self.session_id.unwrap_or_default().to_hex(),
            output: output.to_string(),
            artifacts: artifacts.iter().map(|a| a.id.clone()).collect(),
            timestamp: now_millis(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenClawResponse {
    pub session_id: String,
    pub output: String,
    pub artifacts: Vec<String>,
    pub timestamp: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_openclaw_adapter_session_init() {
        let mut adapter = OpenClawGatewayAdapter::new("http://localhost:3000".into());
        let (sid, state) = adapter.initialize_session("test intent");

        assert_eq!(state.status, SessionStatus::Created);
        assert!(sid.to_hex().len() == 32);
    }

    #[test]
    fn test_bridge_intent() {
        let mut adapter = OpenClawGatewayAdapter::new("http://localhost:3000".into());
        adapter.initialize_session("test");

        let event = adapter.bridge_intent("user message", "discord");
        assert!(matches!(event.event_type, EventType::IntentReceived { .. }));
    }
}
