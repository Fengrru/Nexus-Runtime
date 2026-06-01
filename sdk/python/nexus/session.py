"""
Session — Represents a single Nexus execution session.
"""
import json
import time
from typing import Optional, List, Dict, Any, TYPE_CHECKING
from enum import Enum
from dataclasses import dataclass

if TYPE_CHECKING:
    from .runtime import Runtime

class SessionStatus(Enum):
    CREATED = "created"
    INTAKE = "intake"
    PLANNING = "planning"
    PLANNED = "planned"
    EXECUTING = "executing"
    CHECKPOINTING = "checkpointing"
    BLOCKED = "blocked"
    CONVERGING = "converging"
    REFLECTING = "reflecting"
    COMPLETED = "completed"
    FAILED = "failed"
    ARCHIVED = "archived"

@dataclass
class Session:
    runtime: "Runtime"
    session_id: str
    intent: str
    model: str
    budget_limit_cents: int
    status: SessionStatus = SessionStatus.CREATED
    checkpoint_seq: int = 0

    @property
    def id(self) -> str:
        return self.session_id

    def run(self) -> "Session":
        """Execute the session synchronously (simulated for SDK)."""
        self._transition(SessionStatus.INTAKE)
        self._transition(SessionStatus.PLANNING)
        self._transition(SessionStatus.PLANNED)
        self._transition(SessionStatus.EXECUTING)
        self._checkpoint(1)
        self._transition(SessionStatus.COMPLETED)
        return self

    def _transition(self, status: SessionStatus):
        from .event import NexusEvent
        event = NexusEvent(
            event_id=f"e_{int(time.time()*1000)}_{id(self)}",
            event_type=f"session_{status.value}",
            session_id=self.session_id,
            causal_vector={self.session_id: self.checkpoint_seq + 1},
        )
        self._persist_event(event)
        self.status = status

    def _checkpoint(self, step: int):
        from .event import NexusEvent
        self.checkpoint_seq = step
        event = NexusEvent(
            event_id=f"cp_{int(time.time()*1000)}_{step}",
            event_type="worker_checkpoint",
            session_id=self.session_id,
            causal_vector={self.session_id: step},
        )
        self._persist_event(event)

    def _persist_event(self, event: "NexusEvent"):
        self.runtime._conn.execute(
            """INSERT OR IGNORE INTO events (event_id, event_type, session_id, trace_id,
               causal_vector, payload, payload_hash, event_timestamp, nonce, integrity_hash)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                event.event_id, event.event_type, event.session_id,
                event.trace_id or "0"*32, event.causal_vector,
                event.payload or b"", event.payload_hash or "",
                event.event_timestamp or int(time.time()*1000),
                event.nonce or "0"*32, event.integrity_hash or "",
            ),
        )
        self.runtime._conn.execute(
            """INSERT OR REPLACE INTO sessions (session_id, version, status, checkpoint_seq,
               created_at, updated_at, latest_event_id, intent_graph, execution_frontier,
               memory_refs, budget)
               VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                self.session_id, self.status.value, self.checkpoint_seq,
                int(time.time()*1000), int(time.time()*1000), event.event_id,
                b"", b"", b"", b"",
            ),
        )
        self.runtime._conn.commit()

    def suspend(self):
        self._transition(SessionStatus.CHECKPOINTING)

    def resume(self):
        self._transition(SessionStatus.EXECUTING)

    def block(self, reason: str):
        self._transition(SessionStatus.BLOCKED)

    def approve(self):
        self._transition(SessionStatus.EXECUTING)

    def reject(self):
        self._transition(SessionStatus.FAILED)

    def archive(self):
        self._transition(SessionStatus.ARCHIVED)

    def get_events(self, limit: int = 50) -> List[Dict]:
        return self.runtime.get_events(self.session_id, limit)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "intent": self.intent,
            "model": self.model,
            "status": self.status.value,
            "checkpoint_seq": self.checkpoint_seq,
            "budget_limit_cents": self.budget_limit_cents,
        }
