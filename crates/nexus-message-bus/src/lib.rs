#![deny(clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::sync::Arc;
use nexus_core::*;
use tokio::sync::{broadcast, Mutex, RwLock};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CausalMessage {
    pub message_id: String,
    pub session_id: SessionId,
    pub causal_vector: CausalVector,
    pub topic: String,
    pub payload: Vec<u8>,
    pub timestamp: u64,
    pub origin_node: String,
    pub ttl_hops: u32,
}

impl CausalMessage {
    pub fn new(
        session_id: SessionId,
        causal_vector: CausalVector,
        topic: String,
        payload: Vec<u8>,
        origin_node: String,
    ) -> Self {
        Self {
            message_id: uuid::Uuid::new_v4().to_string(),
            session_id,
            causal_vector,
            topic,
            payload,
            timestamp: now_millis(),
            origin_node,
            ttl_hops: 16,
        }
    }

    pub fn is_expired(&self) -> bool {
        self.ttl_hops == 0
    }

    pub fn decrement_ttl(&mut self) {
        self.ttl_hops = self.ttl_hops.saturating_sub(1);
    }
}

#[derive(Debug, Clone)]
pub struct MessageBusConfig {
    pub node_id: String,
    pub max_buffer_size: usize,
    pub gossip_interval_ms: u64,
    pub peer_nodes: Vec<String>,
}

impl Default for MessageBusConfig {
    fn default() -> Self {
        Self {
            node_id: format!("nexus-node-{}", &uuid::Uuid::new_v4().to_string()[..8]),
            max_buffer_size: 10_000,
            gossip_interval_ms: 500,
            peer_nodes: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct CausalMessageBus {
    config: MessageBusConfig,
    topics: Arc<RwLock<BTreeMap<String, broadcast::Sender<CausalMessage>>>>,
    causal_log: Arc<Mutex<Vec<(String, CausalVector)>>>,
    delivered: Arc<Mutex<Vec<String>>>,
}

impl CausalMessageBus {
    pub fn new(config: MessageBusConfig) -> Self {
        Self {
            config,
            topics: Arc::new(RwLock::new(BTreeMap::new())),
            causal_log: Arc::new(Mutex::new(Vec::new())),
            delivered: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn publish(&self, message: CausalMessage) -> Result<u32, String> {
        let topic = message.topic.clone();

        let topics = self.topics.read().await;
        let sender = topics
            .get(&topic)
            .ok_or_else(|| format!("topic not found: {}", topic))?;

        let recipient_count = sender.receiver_count();
        sender
            .send(message.clone())
            .map_err(|e| format!("broadcast error: {}", e))?;

        let mut log = self.causal_log.lock().await;
        log.push((message.message_id.clone(), message.causal_vector.clone()));

        tracing::info!(
            target = "nexus.bus",
            topic = %topic,
            message_id = %message.message_id,
            recipients = %recipient_count,
            "Message published"
        );

        Ok(recipient_count as u32)
    }

    pub async fn subscribe(&self, topic: &str) -> broadcast::Receiver<CausalMessage> {
        let mut topics = self.topics.write().await;
        let sender = topics
            .entry(topic.to_string())
            .or_insert_with(|| {
                let (tx, _) = broadcast::channel(self.config.max_buffer_size);
                tx
            });
        sender.subscribe()
    }

    pub async fn receive_ordered(&self, rx: &mut broadcast::Receiver<CausalMessage>) -> Vec<CausalMessage> {
        let mut batch = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            batch.push(msg);
        }
        batch.sort_by(|a, b| {
            match a.causal_vector.compare(&b.causal_vector) {
                CausalRelation::Before => std::cmp::Ordering::Less,
                CausalRelation::After => std::cmp::Ordering::Greater,
                CausalRelation::Concurrent => a.timestamp.cmp(&b.timestamp),
            }
        });
        batch
    }

    pub async fn is_delivered(&self, message_id: &str) -> bool {
        let delivered = self.delivered.lock().await;
        delivered.iter().any(|id| id == message_id)
    }

    pub async fn mark_delivered(&self, message_id: String) {
        let mut delivered = self.delivered.lock().await;
        delivered.push(message_id);
    }

    pub async fn check_causal_consistency(
        &self,
        incoming: &CausalVector,
    ) -> Result<(), String> {
        let log = self.causal_log.lock().await;
        for (msg_id, cv) in log.iter() {
            if cv.happened_before(incoming) {
                continue;
            }
            if incoming.happened_before(cv) {
                return Err(format!(
                    "causal gap detected: message {} is concurrent with incoming vector",
                    msg_id
                ));
            }
        }
        Ok(())
    }

    pub fn node_id(&self) -> &str {
        &self.config.node_id
    }

    pub fn peer_count(&self) -> usize {
        self.config.peer_nodes.len()
    }

    pub async fn topic_count(&self) -> usize {
        self.topics.read().await.len()
    }
}

#[derive(Debug, Clone)]
pub struct GossipProtocol {
    bus: Arc<CausalMessageBus>,
    interval_ms: u64,
}

impl GossipProtocol {
    pub fn new(bus: Arc<CausalMessageBus>, interval_ms: u64) -> Self {
        Self { bus, interval_ms }
    }

    pub async fn start(&self) -> tokio::task::JoinHandle<()> {
        let bus = self.bus.clone();
        let interval = self.interval_ms;

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_millis(interval));
            loop {
                ticker.tick().await;

                let msg = CausalMessage::new(
                    SessionId::from_bytes([0xFFu8; 16]),
                    CausalVector::new(),
                    "nexus.gossip.heartbeat".into(),
                    bus.node_id().as_bytes().to_vec(),
                    bus.node_id().into(),
                );

                let _ = bus.publish(msg).await;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_message_publish_subscribe() {
        let config = MessageBusConfig::default();
        let bus = CausalMessageBus::new(config);

        let mut rx = bus.subscribe("test.topic").await;

        let sid = SessionId::from_bytes([1u8; 16]);
        let mut cv = CausalVector::new();
        cv.increment(sid);

        let msg = CausalMessage::new(
            sid,
            cv,
            "test.topic".into(),
            b"hello causal world".to_vec(),
            "node-1".into(),
        );

        let count = bus.publish(msg).await.unwrap();
        assert_eq!(count, 1);

        let received = rx.try_recv().unwrap();
        assert_eq!(received.topic, "test.topic");
        assert_eq!(received.payload, b"hello causal world");
    }

    #[test]
    fn test_message_ttl_decrement() {
        let sid = SessionId::from_bytes([1u8; 16]);
        let mut msg = CausalMessage::new(
            sid,
            CausalVector::new(),
            "test".into(),
            vec![],
            "node-1".into(),
        );
        assert!(!msg.is_expired());
        msg.decrement_ttl();
        assert_eq!(msg.ttl_hops, 15);
    }

    #[tokio::test]
    async fn test_causal_ordering() {
        let config = MessageBusConfig::default();
        let bus = CausalMessageBus::new(config);

        let sid = SessionId::from_bytes([1u8; 16]);
        let mut cv = CausalVector::new();
        cv.increment(sid);
        cv.increment(sid);

        let msg = CausalMessage::new(
            sid,
            cv.clone(),
            "test".into(),
            vec![],
            "node-1".into(),
        );

        let _ = bus.publish(msg).await;

        let mut incoming = CausalVector::new();
        incoming.increment(sid);
        incoming.increment(sid);
        incoming.increment(sid);

        assert!(bus.check_causal_consistency(&incoming).await.is_ok());
    }
}
