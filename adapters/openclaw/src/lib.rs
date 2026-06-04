use nexus_core::*;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone)]
pub struct OpenClawGatewayAdapter {
    session_id: Option<SessionId>,
    gateway_url: String,
    channel: String,
}

impl OpenClawGatewayAdapter {
    pub fn new(gateway_url: String) -> Self {
        Self {
            session_id: None,
            gateway_url,
            channel: "default".into(),
        }
    }

    pub fn with_channel(mut self, channel: &str) -> Self {
        self.channel = channel.to_string();
        self
    }

    pub fn initialize_session(&mut self, intent: &str) -> (SessionId, NexusState) {
        let session_id = SessionId::new();
        let state = NexusState::new(session_id, now_millis());
        self.session_id = Some(session_id);

        tracing::info!(
            target = "nexus.adapter.openclaw",
            session_id = %session_id.to_hex(),
            intent = %intent,
            channel = %self.channel,
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
            channel: self.channel.clone(),
            output: output.to_string(),
            artifacts: artifacts.iter().map(|a| a.id.clone()).collect(),
            timestamp: now_millis(),
        }
    }

    pub fn bridge_to_http_payload(
        &self,
        output: &str,
        artifacts: Vec<ArtifactRef>,
    ) -> Result<(String, String), String> {
        let response = self.bridge_result(output, artifacts);
        let body = serde_json::to_string(&response)
            .map_err(|e| format!("serialize response: {}", e))?;
        let endpoint = format!("{}/sessions/{}/response", self.gateway_url, response.session_id);
        Ok((endpoint, body))
    }

    pub fn gateway_url(&self) -> &str {
        &self.gateway_url
    }

    pub fn channel(&self) -> &str {
        &self.channel
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenClawResponse {
    pub session_id: String,
    pub channel: String,
    pub output: String,
    pub artifacts: Vec<String>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenClawIntent {
    pub intent: String,
    pub channel: String,
    pub metadata: std::collections::BTreeMap<String, String>,
}

impl OpenClawIntent {
    pub fn parse(raw_json: &str) -> Result<Self, String> {
        serde_json::from_str(raw_json)
            .map_err(|e| format!("parse OpenClaw intent: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_openclaw_adapter_session_init() {
        let mut adapter = OpenClawGatewayAdapter::new("http://localhost:3000".into())
            .with_channel("discord");
        let (sid, state) = adapter.initialize_session("test intent");

        assert_eq!(state.status, SessionStatus::Created);
        assert!(sid.to_hex().len() == 32);
        assert_eq!(adapter.channel(), "discord");
    }

    #[test]
    fn test_bridge_intent() {
        let mut adapter = OpenClawGatewayAdapter::new("http://localhost:3000".into());
        adapter.initialize_session("test");

        let event = adapter.bridge_intent("user message", "discord");
        assert!(matches!(event.event_type, EventType::IntentReceived { .. }));
    }

    #[test]
    fn test_bridge_to_http_payload() {
        let mut adapter = OpenClawGatewayAdapter::new("http://localhost:3000".into());
        adapter.initialize_session("test");

        let (endpoint, body) = adapter
            .bridge_to_http_payload("done", vec![])
            .unwrap();
        assert!(endpoint.contains("/sessions/"));
        assert!(endpoint.contains("/response"));
        assert!(body.contains("done"));
    }

    #[test]
    fn test_openclaw_intent_parse() {
        let json = r#"{"intent": "refactor auth", "channel": "discord", "metadata": {"user": "alice"}}"#;
        let intent = OpenClawIntent::parse(json).unwrap();
        assert_eq!(intent.intent, "refactor auth");
        assert_eq!(intent.channel, "discord");
        assert_eq!(intent.metadata.get("user"), Some(&"alice".to_string()));
    }
}
