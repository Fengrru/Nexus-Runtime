"""
Event types for Nexus Runtime.
"""
import uuid
import time
from typing import Optional, Dict, Any
from dataclasses import dataclass, field

@dataclass
class NexusEvent:
    event_id: str = ""
    event_type: str = ""
    session_id: str = ""
    trace_id: Optional[str] = None
    parent_event_id: Optional[str] = None
    causal_vector: Dict[str, int] = field(default_factory=dict)
    payload: Optional[bytes] = None
    payload_hash: str = ""
    event_timestamp: Optional[int] = None
    nonce: Optional[str] = None
    integrity_hash: str = ""

    def __post_init__(self):
        if not self.event_id:
            self.event_id = f"e_{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}"
        if not self.trace_id:
            self.trace_id = uuid.uuid4().hex
        if not self.event_timestamp:
            self.event_timestamp = int(time.time() * 1000)
        if not self.nonce:
            self.nonce = uuid.uuid4().hex

class EventType:
    INTENT_RECEIVED = "intent_received"
    INTENT_PARSED = "intent_parsed"
    PLAN_PROPOSED = "plan_proposed"
    PLAN_COMMITTED = "plan_committed"
    PLAN_REJECTED = "plan_rejected"
    DEPENDENCIES_MET = "dependencies_met"
    FRONTIER_VALIDATED = "frontier_validated"
    WORKER_DISPATCHED = "worker_dispatched"
    WORKER_STARTED = "worker_started"
    WORKER_CHECKPOINT = "worker_checkpoint"
    WORKER_COMPLETED = "worker_completed"
    WORKER_FAILED = "worker_failed"
    CONVERGE_STARTED = "converge_started"
    CONVERGE_COMPLETE = "converge_complete"
    REFLECTION_STARTED = "reflection_started"
    REFLECTION_COMPLETE = "reflection_complete"
    MEMORY_CONSOLIDATED = "memory_consolidated"
    SIDE_EFFECT_INTENT = "side_effect_intent"
    SIDE_EFFECT_COMMITTED = "side_effect_committed"
    SIDE_EFFECT_COMPENSATED = "side_effect_compensated"
    HUMAN_APPROVAL_REQUESTED = "human_approval_requested"
    HUMAN_APPROVED = "human_approved"
    HUMAN_REJECTED = "human_rejected"
    POLICY_DECISION = "policy_decision"
    SESSION_SUSPENDED = "session_suspended"
    SESSION_RESUMED = "session_resumed"
    SESSION_MIGRATED = "session_migrated"
    SESSION_ARCHIVED = "session_archived"
