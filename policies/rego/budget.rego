package nexus.policy

# Hot-path budget policy (< 1ms evaluation)
# Evaluated before every LLM call and side-effect execution.

default allow = false

allow {
    input.action == "llm_call"
    input.cost_cents <= input.budget_remaining
    input.session_status != "blocked"
}

allow {
    input.action == "side_effect"
    input.effect_class != "irreversible"
    input.capability_valid == true
}

allow {
    input.action == "human_override"
    input.approver != ""
    input.timestamp < input.timeout
}

# Deny if budget exceeded
deny_budget[msg] {
    input.action == "llm_call"
    input.cost_cents > input.budget_remaining
    msg := sprintf("Budget exceeded: need %d, have %d", [input.cost_cents, input.budget_remaining])
}

# Deny if session is blocked
deny_blocked[msg] {
    input.session_status == "blocked"
    msg := "Session is blocked pending human review"
}

# Deny if capability token is expired
deny_capability[msg] {
    input.capability_expired == true
    msg := sprintf("Capability token expired at %d", [input.token_expiry])
}
