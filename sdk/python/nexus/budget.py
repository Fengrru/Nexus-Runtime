"""
Budget — Cost governance for sessions.
"""
from dataclasses import dataclass

@dataclass
class Budget:
    limit_cents: int = 500
    consumed_cents: int = 0
    token_count: int = 0
    tool_call_count: int = 0

    @property
    def remaining_cents(self) -> int:
        return max(0, self.limit_cents - self.consumed_cents)

    @property
    def remaining_dollars(self) -> float:
        return self.remaining_cents / 100.0

    @property
    def is_exhausted(self) -> bool:
        return self.consumed_cents >= self.limit_cents

    @property
    def usage_percent(self) -> float:
        if self.limit_cents == 0:
            return 100.0
        return (self.consumed_cents / self.limit_cents) * 100.0

    def add_cost(self, cents: int, tokens: int = 0, tool_calls: int = 0):
        self.consumed_cents = min(self.consumed_cents + cents, self.limit_cents)
        self.token_count += tokens
        self.tool_call_count += tool_calls

    def can_afford(self, estimated_cents: int) -> bool:
        return self.consumed_cents + estimated_cents <= self.limit_cents

    def reset(self):
        self.consumed_cents = 0
        self.token_count = 0
        self.tool_call_count = 0

    def to_dict(self) -> dict:
        return {
            "limit_cents": self.limit_cents,
            "consumed_cents": self.consumed_cents,
            "token_count": self.token_count,
            "tool_call_count": self.tool_call_count,
            "remaining": self.remaining_cents,
            "usage_percent": round(self.usage_percent, 1),
        }
