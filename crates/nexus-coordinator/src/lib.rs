#![deny(clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::sync::Arc;
use nexus_core::*;
use nexus_event_store::EventStore;
use serde::{Serialize, Deserialize};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CoordinationPhase {
    Propose,
    Validate,
    Commit,
    Abort,
    Converge,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinationMessage {
    pub phase: CoordinationPhase,
    pub coordinator_id: SessionId,
    pub participant_ids: Vec<SessionId>,
    pub proposal: Proposal,
    pub causal_vector: CausalVector,
    pub timestamp: u64,
    pub message_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Proposal {
    pub intent: String,
    pub task_ids: Vec<TaskId>,
    pub expected_outputs: BTreeMap<TaskId, String>,
    pub constraints: Vec<Constraint>,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinationVote {
    pub voter_id: SessionId,
    pub approved: bool,
    pub reason: Option<String>,
    pub counter_proposal: Option<Proposal>,
    pub causal_vector: CausalVector,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinationResult {
    pub success: bool,
    pub committed_at: u64,
    pub participant_results: BTreeMap<SessionId, bool>,
    pub conflicts: Vec<String>,
    pub convergence_cv: CausalVector,
}

pub struct MultiAgentCoordinator<S: EventStore> {
    store: Arc<S>,
    active_sessions: Arc<RwLock<BTreeMap<SessionId, CoordinationState>>>,
    node_id: String,
    quorum_size: usize,
}

#[derive(Debug, Clone)]
struct CoordinationState {
    phase: CoordinationPhase,
    votes: BTreeMap<SessionId, CoordinationVote>,
    #[allow(dead_code)]
    proposal: Proposal,
    #[allow(dead_code)]
    started_at: u64,
}

impl<S: EventStore> MultiAgentCoordinator<S> {
    pub fn new(store: Arc<S>, node_id: String, quorum_size: usize) -> Self {
        Self {
            store,
            active_sessions: Arc::new(RwLock::new(BTreeMap::new())),
            node_id,
            quorum_size,
        }
    }

    pub async fn propose_coordination(
        &self,
        coordinator_id: SessionId,
        participant_ids: Vec<SessionId>,
        proposal: Proposal,
    ) -> Result<CoordinationMessage, String> {
        let mut cv = CausalVector::new();
        cv.increment(coordinator_id);

        for pid in &participant_ids {
            cv.increment(*pid);
        }

        let msg = CoordinationMessage {
            phase: CoordinationPhase::Propose,
            coordinator_id,
            participant_ids: participant_ids.clone(),
            proposal: proposal.clone(),
            causal_vector: cv,
            timestamp: now_millis(),
            message_id: uuid::Uuid::new_v4().to_string(),
        };

        let mut sessions = self.active_sessions.write().await;
        sessions.insert(
            coordinator_id,
            CoordinationState {
                phase: CoordinationPhase::Propose,
                votes: BTreeMap::new(),
                proposal,
                started_at: now_millis(),
            },
        );

        tracing::info!(
            target = "nexus.coordinator",
            coordinator = %coordinator_id.to_hex(),
            participants = %participant_ids.len(),
            "Coordination proposed"
        );

        Ok(msg)
    }

    pub async fn vote(
        &self,
        voter_id: SessionId,
        vote: CoordinationVote,
    ) -> Result<bool, String> {
        let mut sessions = self.active_sessions.write().await;
        let state = sessions
            .values_mut()
            .find(|s| matches!(s.phase, CoordinationPhase::Propose))
            .ok_or("no active coordination round")?;

        state.votes.insert(voter_id, vote);

        let _total_participants = state.votes.len();
        let approved = state.votes.values().filter(|v| v.approved).count();

        Ok(approved >= self.quorum_size)
    }

    pub async fn has_quorum(&self) -> bool {
        let sessions = self.active_sessions.read().await;
        sessions.values().any(|s| {
            let approved = s.votes.values().filter(|v| v.approved).count();
            approved >= self.quorum_size
        })
    }

    pub async fn commit_coordination(
        &self,
    ) -> Result<CoordinationResult, String> {
        let mut sessions = self.active_sessions.write().await;
        let state = sessions
            .values_mut()
            .find(|s| matches!(s.phase, CoordinationPhase::Propose))
            .ok_or("no active coordination")?;

        let approved = state.votes.values().filter(|v| v.approved).count();
        let success = approved >= self.quorum_size;

        state.phase = if success {
            CoordinationPhase::Commit
        } else {
            CoordinationPhase::Abort
        };

        let mut participant_results = BTreeMap::new();
        let mut conflicts = Vec::new();
        let mut convergence_cv = CausalVector::new();

        for (pid, vote) in &state.votes {
            participant_results.insert(*pid, vote.approved);
            convergence_cv.merge(&vote.causal_vector);
            if !vote.approved {
                if let Some(ref reason) = vote.reason {
                    conflicts.push(format!("{}: {}", pid.to_hex(), reason));
                }
            }
        }

        let result = CoordinationResult {
            success,
            committed_at: now_millis(),
            participant_results,
            conflicts,
            convergence_cv,
        };

        tracing::info!(
            target = "nexus.coordinator",
            success = %result.success,
            conflicts = %result.conflicts.len(),
            "Coordination committed"
        );

        Ok(result)
    }

    pub async fn converge_results(
        &self,
        session_ids: Vec<SessionId>,
    ) -> Result<CausalVector, String> {
        let mut merged_cv = CausalVector::new();

        for sid in &session_ids {
            merged_cv.increment(*sid);
        }

        let events: Vec<NexusEvent> = Vec::new();
        for sid in session_ids {
            if let Ok(evts) = self.store.get_events(sid, None).await {
                for event in evts {
                    merged_cv.merge(&event.causal_vector);
                }
            }
        }

        tracing::info!(
            target = "nexus.coordinator.converge",
            events = %events.len(),
            "Results converged"
        );

        Ok(merged_cv)
    }

    pub fn quorum_size(&self) -> usize {
        self.quorum_size
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }
}

pub struct CoordinationPolicy {
    pub min_quorum_pct: f64,
    pub max_timeout_ms: u64,
    pub auto_approve_if_single_participant: bool,
}

impl Default for CoordinationPolicy {
    fn default() -> Self {
        Self {
            min_quorum_pct: 0.67,
            max_timeout_ms: 30_000,
            auto_approve_if_single_participant: true,
        }
    }
}

impl CoordinationPolicy {
    pub fn quorum_for(&self, participant_count: usize) -> usize {
        if self.auto_approve_if_single_participant && participant_count <= 1 {
            return 1;
        }
        ((participant_count as f64 * self.min_quorum_pct).ceil() as usize).max(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_event_store::SqliteEventStore;

    async fn setup_coordinator() -> MultiAgentCoordinator<SqliteEventStore> {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("coordinator_test.db");
        let db_url = format!("sqlite:{}?mode=rwc", db_path.display());
        let store = SqliteEventStore::new(&db_url).await.unwrap();
        MultiAgentCoordinator::new(Arc::new(store), "node-1".into(), 2)
    }

    #[tokio::test]
    async fn test_propose_and_vote() {
        let coordinator = setup_coordinator().await;
        let coord_id = SessionId::from_bytes([1u8; 16]);
        let p1 = SessionId::from_bytes([2u8; 16]);
        let p2 = SessionId::from_bytes([3u8; 16]);

        let proposal = Proposal {
            intent: "multi-agent refactor".into(),
            task_ids: vec![TaskId::from_bytes([10u8; 16])],
            expected_outputs: BTreeMap::new(),
            constraints: vec![Constraint {
                constraint_type: "no_side_effects".into(),
                value: "true".into(),
            }],
            timeout_ms: 30_000,
        };

        let msg = coordinator
            .propose_coordination(coord_id, vec![p1, p2], proposal)
            .await
            .unwrap();
        assert_eq!(msg.phase, CoordinationPhase::Propose);

        let mut cv = CausalVector::new();
        cv.increment(p1);

        let vote = CoordinationVote {
            voter_id: p1,
            approved: true,
            reason: None,
            counter_proposal: None,
            causal_vector: cv,
        };

        let has_quorum = coordinator.vote(p1, vote).await.unwrap();
        assert!(!has_quorum);

        let mut cv2 = CausalVector::new();
        cv2.increment(p2);

        let vote2 = CoordinationVote {
            voter_id: p2,
            approved: true,
            reason: None,
            counter_proposal: None,
            causal_vector: cv2,
        };

        let has_quorum2 = coordinator.vote(p2, vote2).await.unwrap();
        assert!(has_quorum2);

        let result = coordinator.commit_coordination().await.unwrap();
        assert!(result.success);
        assert_eq!(result.participant_results.len(), 2);
    }

    #[test]
    fn test_quorum_policy() {
        let policy = CoordinationPolicy::default();
        assert_eq!(policy.quorum_for(1), 1);
        assert_eq!(policy.quorum_for(3), 3);
        assert_eq!(policy.quorum_for(5), 4);
        assert_eq!(policy.quorum_for(10), 7);
    }

    #[tokio::test]
    async fn test_coordination_failure_with_insufficient_votes() {
        let coordinator = setup_coordinator().await;
        let coord_id = SessionId::from_bytes([1u8; 16]);
        let p1 = SessionId::from_bytes([2u8; 16]);
        let p2 = SessionId::from_bytes([3u8; 16]);

        let proposal = Proposal {
            intent: "will fail".into(),
            task_ids: vec![],
            expected_outputs: BTreeMap::new(),
            constraints: vec![],
            timeout_ms: 30_000,
        };

        coordinator
            .propose_coordination(coord_id, vec![p1, p2], proposal)
            .await
            .unwrap();

        let vote = CoordinationVote {
            voter_id: p1,
            approved: false,
            reason: Some("unsafe operation".into()),
            counter_proposal: None,
            causal_vector: CausalVector::singleton(p1, 1),
        };

        coordinator.vote(p1, vote).await.unwrap();

        let result = coordinator.commit_coordination().await.unwrap();
        assert!(!result.success);
        assert_eq!(result.conflicts.len(), 1);
    }
}
