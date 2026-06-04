#![deny(clippy::disallowed_types)]

use prometheus::{
    self, CounterVec, GaugeVec, HistogramVec, Registry, Encoder, TextEncoder,
    Opts, HistogramOpts,
};

#[derive(Clone)]
pub struct NexusMetrics {
    pub registry: Registry,

    pub events_appended: CounterVec,
    pub transitions: CounterVec,
    pub workers_spawned: CounterVec,
    pub workers_failed: CounterVec,
    pub recovery_duration: HistogramVec,
    pub checkpoint_size: HistogramVec,
    pub side_effects_committed: CounterVec,
    pub llm_calls: CounterVec,
    pub llm_cost_cents: CounterVec,
    pub memory_graph_nodes: GaugeVec,
    pub entropy_score: GaugeVec,
    pub sessions_active: GaugeVec,
    pub causal_conflicts: CounterVec,
    pub coordination_rounds: CounterVec,
    pub message_bus_messages: CounterVec,
}

impl NexusMetrics {
    pub fn new() -> Result<Self, prometheus::Error> {
        let registry = Registry::new();

        let events_appended = CounterVec::new(
            Opts::new("nexus_events_appended_total", "Total events appended to the event log"),
            &["event_type", "session_status"],
        )?;
        registry.register(Box::new(events_appended.clone()))?;

        let transitions = CounterVec::new(
            Opts::new("nexus_transitions_total", "Total state machine transitions"),
            &["from_status", "to_status"],
        )?;
        registry.register(Box::new(transitions.clone()))?;

        let workers_spawned = CounterVec::new(
            Opts::new("nexus_workers_spawned_total", "Total workers spawned"),
            &["worker_type"],
        )?;
        registry.register(Box::new(workers_spawned.clone()))?;

        let workers_failed = CounterVec::new(
            Opts::new("nexus_workers_failed_total", "Total worker failures"),
            &["error_code"],
        )?;
        registry.register(Box::new(workers_failed.clone()))?;

        let recovery_duration = HistogramVec::new(
            HistogramOpts::new("nexus_recovery_duration_ms", "Recovery duration in milliseconds"),
            &["events_replayed"],
        )?;
        registry.register(Box::new(recovery_duration.clone()))?;

        let checkpoint_size = HistogramVec::new(
            HistogramOpts::new("nexus_checkpoint_size_bytes", "Checkpoint size in bytes"),
            &["step_index"],
        )?;
        registry.register(Box::new(checkpoint_size.clone()))?;

        let side_effects_committed = CounterVec::new(
            Opts::new("nexus_side_effects_committed_total", "Total side effects committed"),
            &["effect_class"],
        )?;
        registry.register(Box::new(side_effects_committed.clone()))?;

        let llm_calls = CounterVec::new(
            Opts::new("nexus_llm_calls_total", "Total LLM calls"),
            &["model"],
        )?;
        registry.register(Box::new(llm_calls.clone()))?;

        let llm_cost_cents = CounterVec::new(
            Opts::new("nexus_llm_cost_cents_total", "Total LLM cost in cents"),
            &["model", "session_id"],
        )?;
        registry.register(Box::new(llm_cost_cents.clone()))?;

        let memory_graph_nodes = GaugeVec::new(
            Opts::new("nexus_memory_graph_nodes", "Number of nodes in the memory graph"),
            &["session_id"],
        )?;
        registry.register(Box::new(memory_graph_nodes.clone()))?;

        let entropy_score = GaugeVec::new(
            Opts::new("nexus_entropy_score", "Current entropy score"),
            &[],
        )?;
        registry.register(Box::new(entropy_score.clone()))?;

        let sessions_active = GaugeVec::new(
            Opts::new("nexus_sessions_active", "Number of active sessions"),
            &["status"],
        )?;
        registry.register(Box::new(sessions_active.clone()))?;

        let causal_conflicts = CounterVec::new(
            Opts::new("nexus_causal_conflicts_total", "Total causal conflicts detected"),
            &["type"],
        )?;
        registry.register(Box::new(causal_conflicts.clone()))?;

        let coordination_rounds = CounterVec::new(
            Opts::new("nexus_coordination_rounds_total", "Total multi-agent coordination rounds"),
            &["outcome"],
        )?;
        registry.register(Box::new(coordination_rounds.clone()))?;

        let message_bus_messages = CounterVec::new(
            Opts::new("nexus_message_bus_messages_total", "Total messages on the causal bus"),
            &["topic", "direction"],
        )?;
        registry.register(Box::new(message_bus_messages.clone()))?;

        Ok(Self {
            registry,
            events_appended,
            transitions,
            workers_spawned,
            workers_failed,
            recovery_duration,
            checkpoint_size,
            side_effects_committed,
            llm_calls,
            llm_cost_cents,
            memory_graph_nodes,
            entropy_score,
            sessions_active,
            causal_conflicts,
            coordination_rounds,
            message_bus_messages,
        })
    }

    pub fn record_event_append(&self, event_type: &str, session_status: &str) {
        self.events_appended
            .with_label_values(&[event_type, session_status])
            .inc();
    }

    pub fn record_transition(&self, from: &str, to: &str) {
        self.transitions
            .with_label_values(&[from, to])
            .inc();
    }

    pub fn record_worker_spawn(&self, worker_type: &str) {
        self.workers_spawned
            .with_label_values(&[worker_type])
            .inc();
    }

    pub fn record_worker_failure(&self, error_code: &str) {
        self.workers_failed
            .with_label_values(&[error_code])
            .inc();
    }

    pub fn record_recovery(&self, events_replayed: usize, duration_ms: u64) {
        self.recovery_duration
            .with_label_values(&[&events_replayed.to_string()])
            .observe(duration_ms as f64);
    }

    pub fn record_side_effect(&self, effect_class: &str) {
        self.side_effects_committed
            .with_label_values(&[effect_class])
            .inc();
    }

    pub fn record_llm_call(&self, model: &str, session_id: &str, cost_cents: u64) {
        self.llm_calls
            .with_label_values(&[model])
            .inc();
        self.llm_cost_cents
            .with_label_values(&[model, session_id])
            .inc_by(cost_cents as f64);
    }

    pub fn set_memory_graph_size(&self, session_id: &str, node_count: u64) {
        self.memory_graph_nodes
            .with_label_values(&[session_id])
            .set(node_count as f64);
    }

    pub fn set_entropy_score(&self, score: f64) {
        self.entropy_score
            .with_label_values(&[])
            .set(score);
    }

    pub fn set_sessions_active(&self, status: &str, count: u64) {
        self.sessions_active
            .with_label_values(&[status])
            .set(count as f64);
    }

    pub fn record_causal_conflict(&self, conflict_type: &str) {
        self.causal_conflicts
            .with_label_values(&[conflict_type])
            .inc();
    }

    pub fn record_coordination(&self, outcome: &str) {
        self.coordination_rounds
            .with_label_values(&[outcome])
            .inc();
    }

    pub fn record_message(&self, topic: &str, direction: &str) {
        self.message_bus_messages
            .with_label_values(&[topic, direction])
            .inc();
    }

    pub fn export_text(&self) -> Result<String, prometheus::Error> {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer)?;
        String::from_utf8(buffer).map_err(|e| {
            prometheus::Error::Msg(format!("UTF-8 error: {}", e))
        })
    }
}

impl Default for NexusMetrics {
    fn default() -> Self {
        Self::new().expect("Failed to initialize NexusMetrics")
    }
}

pub struct MetricsExporter {
    metrics: NexusMetrics,
}

impl MetricsExporter {
    pub fn new(metrics: NexusMetrics) -> Self {
        Self { metrics }
    }

    pub async fn serve(&self) -> Result<(), String> {
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            tracing::info!("nexus.metrics exporter started");
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(15)).await;
                if let Ok(text) = metrics.export_text() {
                    tracing::debug!(bytes = %text.len(), "Metrics snapshot ready");
                }
            }
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_initialization() {
        let metrics = NexusMetrics::new().unwrap();
        metrics.record_event_append("intent_received", "created");
        metrics.record_transition("created", "intake");
        metrics.record_worker_spawn("python");
        metrics.record_side_effect("idempotent");
        metrics.record_llm_call("claude-3.5-sonnet", "s1", 150);
        metrics.set_entropy_score(0.35);
        metrics.record_causal_conflict("concurrent");
        metrics.record_coordination("committed");

        let text = metrics.export_text().unwrap();
        assert!(text.contains("nexus_events_appended_total"));
        assert!(text.contains("nexus_entropy_score"));
    }

    #[test]
    fn test_session_gauge_updates() {
        let metrics = NexusMetrics::new().unwrap();
        metrics.set_sessions_active("executing", 5);
        metrics.set_sessions_active("completed", 10);

        let text = metrics.export_text().unwrap();
        assert!(text.contains("nexus_sessions_active"));
    }
}
