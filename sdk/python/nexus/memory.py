"""
Memory — Causal memory graph for cross-session knowledge inheritance.
"""
import time
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field
from enum import Enum

class MemoryEdgeType(Enum):
    DERIVES_FROM = "derives_from"
    CONTRADICTS = "contradicts"
    REFINES = "refines"
    GENERALIZES = "generalizes"
    ENABLES = "enables"
    CAUSED_BY = "caused_by"
    SIMILAR_TO = "similar_to"
    PART_OF = "part_of"

class MemoryContentType(Enum):
    TEXT = "text"
    STRUCTURED = "structured"
    PROPOSITION = "proposition"
    SKILL = "skill"

@dataclass
class MemoryContent:
    content_type: MemoryContentType = MemoryContentType.TEXT
    text: str = ""
    data: Dict[str, str] = field(default_factory=dict)
    subject: str = ""
    predicate: str = ""
    object: str = ""
    confidence: int = 5000

    def matches_goal(self, goal: str) -> bool:
        search_text = self.text + " " + " ".join(self.data.values())
        search_text += f" {self.subject} {self.predicate} {self.object}"
        return goal.lower() in search_text.lower()

@dataclass
class Memory:
    memory_id: str
    content: MemoryContent
    session_origin: str = ""
    causal_vector: Dict[str, int] = field(default_factory=dict)
    importance: int = 500
    created_at: int = 0

    @property
    def id(self) -> str:
        return self.memory_id

    def to_dict(self) -> dict:
        return {
            "memory_id": self.memory_id,
            "content_type": self.content.content_type.value,
            "text": self.content.text,
            "session_origin": self.session_origin,
            "importance": self.importance,
            "causal_vector": self.causal_vector,
            "created_at": self.created_at,
        }

class MemoryGraph:
    def __init__(self):
        self.nodes: Dict[str, Memory] = {}
        self.edges: List[Dict[str, Any]] = []

    def add(self, memory: Memory):
        if memory.created_at == 0:
            memory.created_at = int(time.time() * 1000)
        self.nodes[memory.memory_id] = memory

    def add_edge(self, from_id: str, to_id: str, edge_type: MemoryEdgeType, confidence: int = 5000):
        self.edges.append({
            "from": from_id,
            "to": to_id,
            "edge_type": edge_type.value,
            "confidence": confidence,
        })

    def get(self, memory_id: str) -> Optional[Memory]:
        return self.nodes.get(memory_id)

    def query_causal(self, from_id: str, edge_type: Optional[MemoryEdgeType] = None, depth: int = 3) -> List[Memory]:
        visited = set()
        results = []
        queue = [(from_id, 0)]

        while queue:
            current, d = queue.pop(0)
            if d > depth or current in visited:
                continue
            visited.add(current)

            if current in self.nodes:
                results.append(self.nodes[current])

            for edge in self.edges:
                if edge["from"] == current:
                    if edge_type is None or edge["edge_type"] == edge_type.value:
                        queue.append((edge["to"], d + 1))

        return results

    def compute_activation(self, memory_id: str, query_context: dict) -> int:
        node = self.nodes.get(memory_id)
        if node is None:
            return 0

        relevance = 5000
        importance = node.importance
        recency = min(10000, 5000)
        goal_alignment = 3000
        causal_proximity = 5000

        return (relevance * 3000 + importance * 2500 + recency * 2000
                + goal_alignment * 1500 + causal_proximity * 1000) // 10000

    def inherit_from(self, other: "MemoryGraph", source_session: str):
        count = 0
        for mem_id, memory in other.nodes.items():
            new_id = f"{source_session}:{mem_id}" if not mem_id.startswith(source_session) else mem_id
            new_memory = Memory(
                memory_id=new_id,
                content=memory.content,
                session_origin=source_session,
                causal_vector=memory.causal_vector,
                importance=memory.importance,
                created_at=memory.created_at,
            )
            self.nodes[new_memory.memory_id] = new_memory
            count += 1

        for edge in other.edges:
            new_edge = dict(edge)
            new_edge["from"] = f"{source_session}:{edge['from']}" if not edge["from"].startswith(source_session) else edge["from"]
            new_edge["to"] = f"{source_session}:{edge['to']}" if not edge["to"].startswith(source_session) else edge["to"]
            self.edges.append(new_edge)

        return count

    def size(self) -> int:
        return len(self.nodes)

    def to_dict(self) -> dict:
        return {
            "node_count": len(self.nodes),
            "edge_count": len(self.edges),
            "nodes": [m.to_dict() for m in self.nodes.values()],
            "edges": self.edges,
        }
