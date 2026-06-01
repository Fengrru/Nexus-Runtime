package nexus.capabilities

# Capability token validation policy

default allow = false

allow {
    input.token_signature_valid == true
    input.token_not_expired == true
    input.session_matches == true
    input.task_matches == true
    input.scope_covers_request == true
    input.path_canonicalized == true
}

# All conditions must pass
token_signature_valid {
    input.token_signature == input.expected_signature
}

token_not_expired {
    input.token_expires_at > input.now
}

session_matches {
    input.token_session_id == input.request_session_id
}

task_matches {
    input.token_task_id == input.request_task_id
}

scope_covers_request {
    some scope in input.token_scopes
    input.requested_action == scope
}

path_canonicalized {
    not contains(input.requested_path, "../")
    not contains(input.requested_path, "..\\")
}

# Deny reasons
deny_expired[msg] {
    input.token_expires_at <= input.now
    msg := "Token expired"
}

deny_signature[msg] {
    input.token_signature != input.expected_signature
    msg := "Invalid token signature"
}

deny_scope[msg] {
    not scope_covers_request
    msg := sprintf("Token does not cover action: %s", [input.requested_action])
}
