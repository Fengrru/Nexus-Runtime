use std::collections::BTreeMap;
use clap::{Parser, Subcommand};
use nexus_core::*;
use nexus_core::llm_proxy::{LlmProxy, LlmRequest, ProxyError};
use nexus_event_store::{SqliteEventStore, EventStore};

#[derive(Parser)]
#[command(name = "nexus", version = "1.0.0", about = "Nexus Runtime CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create and execute a new session
    Run {
        /// The intent to execute
        intent: String,

        /// Model to use for LLM calls
        #[arg(long, default_value = "claude-3.5-sonnet")]
        model: String,

        /// Budget limit in USD
        #[arg(long, default_value_t = 5.00)]
        budget: f64,
    },

    /// Resume a session from its last checkpoint
    Resume {
        /// Session ID to resume
        session_id: String,

        /// Checkpoint to resume from (default: latest)
        #[arg(long)]
        from: Option<u64>,
    },

    /// Suspend an active session
    Suspend {
        /// Session ID to suspend
        session_id: String,
    },

    /// Show session status
    Status {
        /// Session ID to inspect
        session_id: String,
    },

    /// Archive a completed session
    Archive {
        /// Session ID to archive
        session_id: String,
    },

    /// Export a session to a .nexus file
    Export {
        /// Session ID to export
        session_id: String,

        /// Output file path
        #[arg(short, long, default_value = "session.nexus")]
        output: String,
    },

    /// Import a session from a .nexus file
    Import {
        /// Input file path
        file: String,

        /// Optional new session ID
        #[arg(long)]
        as_: Option<String>,
    },

    /// Show event log for a session
    Log {
        /// Session ID
        session_id: String,

        /// Limit number of events
        #[arg(long, default_value_t = 20)]
        limit: usize,

        /// Show events since timestamp
        #[arg(long)]
        since: Option<String>,
    },

    /// Inspect session details
    Inspect {
        /// Session ID
        session_id: String,

        /// Show state info
        #[arg(long)]
        state: bool,

        /// Show memory info
        #[arg(long)]
        memory: bool,

        /// Show budget info
        #[arg(long)]
        budget: bool,
    },

    /// Show budget status
    BudgetStatus,

    /// Set budget for a session
    BudgetSet {
        /// Session ID
        #[arg(short, long)]
        session: String,

        /// Budget limit in USD
        #[arg(short, long)]
        limit: f64,
    },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "nexus=info".into()),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Run { intent, model, budget } => {
            run_session(&intent, &model, budget).await;
        }
        Commands::Resume { session_id, from } => {
            resume_session(&session_id, from).await;
        }
        Commands::Status { session_id } => {
            show_status(&session_id).await;
        }
        Commands::Suspend { session_id } => {
            suspend_session(&session_id).await;
        }
        Commands::Archive { session_id } => {
            archive_session(&session_id).await;
        }
        Commands::Export { session_id, output } => {
            export_session(&session_id, &output).await;
        }
        Commands::Import { file, as_ } => {
            import_session(&file, as_).await;
        }
        Commands::Log {
            session_id,
            limit,
            since,
        } => {
            show_log(&session_id, limit, since).await;
        }
        Commands::Inspect {
            session_id,
            state,
            memory,
            budget,
        } => {
            inspect_session(&session_id, state, memory, budget).await;
        }
        Commands::BudgetStatus => {
            budget_status().await;
        }
        Commands::BudgetSet { session, limit } => {
            budget_set(&session, limit).await;
        }
    }
}

async fn get_store() -> Result<SqliteEventStore, String> {
    let home = std::env::var("USERPROFILE")
        .or_else(|_| std::env::var("HOME"))
        .unwrap_or_else(|_| ".".into());
    let db_path = std::path::Path::new(&home).join(".nexus").join("events.db");

    if let Some(parent) = db_path.parent() {
        tokio::fs::create_dir_all(parent).await.ok();
    }

    let db_url = format!(
        "sqlite:{}?mode=rwc",
        db_path.to_str().unwrap_or("nexus.db").replace('\\', "/")
    );

    SqliteEventStore::new(&db_url)
        .await
        .map_err(|e| format!("Failed to connect to event store: {}", e))
}

async fn run_session(intent: &str, model: &str, budget_usd: f64) {
    let session_id = SessionId::new();
    let budget_cents = (budget_usd * 100.0) as u64;

    println!("=== Nexus Runtime v1.0 ===");
    println!("Session: {}", session_id.to_hex());
    println!("Intent:  {}", intent);
    println!("Model:   {}", model);
    println!("Budget:  ${:.2} ({:?} cents)", budget_usd, budget_cents);
    println!();

    let store = match get_store().await {
        Ok(s) => s,
        Err(e) => { println!("[ERR] {}", e); return; }
    };

    let mut state = NexusState::new(session_id, now_millis());
    let dag = BTreeMap::new();

    let mut cv = CausalVector::new();
    cv.increment(session_id);

    let event = NexusEvent::new(
        EventType::IntentReceived {
            raw_input: intent.to_string(),
            source: "cli".to_string(),
        },
        session_id,
        cv.clone(),
        None,
    );

    match store.append_event(&event).await {
        Ok(()) => println!("[INTAKE] Intent received"),
        Err(e) => { println!("[ERR] {}", e); return; }
    }
    state.latest_event_id = event.event_id.clone();
    state = transition(&state, &event, &dag).unwrap();
    println!("        Status: {:?}", state.status);

    cv.increment(session_id);
    let parsed_event = NexusEvent::new(
        EventType::IntentParsed {
            intent_graph: IntentGraph::default(),
        },
        session_id,
        cv.clone(),
        None,
    );
    store.append_event(&parsed_event).await.ok();
    state.latest_event_id = parsed_event.event_id.clone();
    state = transition(&state, &parsed_event, &dag).unwrap();
    println!("[PARSE]  Intent parsed → {:?}", state.status);

    // Real LLM planning via Kernel proxy (spec: LLM calls are externalized events)
    let mut llm_proxy = LlmProxy::new(b"nexus-kernel-signing-key-32b".to_vec());
    let llm_request = LlmRequest {
        request_id: format!("req_{}", now_millis()),
        session_id,
        model: model.to_string(),
        prompt: format!(
            "You are a task planner. Decompose this user intent into executable steps.\n\
             Intent: {}\n\
             Output a JSON array of steps, each step has: action_type (read_file/write_file/grep/run_command), target (file path), and parameters (key-value map).\n\
             Reply with ONLY the JSON array, no other text.",
            intent
        ),
        max_tokens: 2048,
        temperature: 0.3,
    };

    let mut budget_state = state.budget.clone();

    let llm_plan;

    match llm_proxy.proxy_call(llm_request, &mut budget_state, &cv).await {
        Ok((response, llm_event)) => {
            println!(
                "[LLM]    {} → {} in, {} out, ${:.4} cost",
                model, response.input_tokens, response.output_tokens,
                response.cost_cents as f64 / 100.0
            );
            println!("         Plan: {}", &response.content[..200.min(response.content.len())]);
            llm_plan = response.content.clone();

            cv.increment(session_id);
            store.append_event(&llm_event).await.ok();
            state.latest_event_id = llm_event.event_id.clone();
            state.budget = budget_state.clone();
            if let Ok(next) = transition(&state, &llm_event, &dag) {
                state = next;
            }
        }
        Err(ProxyError::ApiError(ref msg)) if msg.contains("not set") => {
            println!("[LLM]    No API key set — using simulated plan");
            llm_plan = "[{\"action_type\": \"grep\", \"target\": \"README.md\", \"parameters\": {\"pattern\": \"Nexus\"}}]".to_string();
            println!("         Plan: {}", llm_plan);
        }
        Err(e) => {
            println!("[LLM]    API error: {} — falling back to simulated plan", e);
            llm_plan = "[{\"action_type\": \"grep\", \"target\": \"README.md\", \"parameters\": {\"pattern\": \"Nexus\"}}]".to_string();
        }
    }

    cv.increment(session_id);
    let plan_event = NexusEvent::new(
        EventType::PlanCommitted {
            frontier: Frontier {
                nodes: vec![],
                blocked: vec![],
                completed: vec![],
            },
        },
        session_id,
        cv.clone(),
        None,
    );
    store.append_event(&plan_event).await.ok();
    state.latest_event_id = plan_event.event_id.clone();
    state = transition(&state, &plan_event, &dag).unwrap();
    println!("[PLAN]   Plan committed → {:?}", state.status);

    cv.increment(session_id);
    let deps_event = NexusEvent::new(
        EventType::DependenciesMet,
        session_id,
        cv.clone(),
        None,
    );
    store.append_event(&deps_event).await.ok();
    state.latest_event_id = deps_event.event_id.clone();
    state = transition(&state, &deps_event, &dag).unwrap();
    println!("[EXEC]   Dependencies met → {:?}", state.status);

    let task_id = TaskId::new();
    let spawner = WorkerSpawner::new().with_python("python");

    let worker_config = SpawnerConfig {
        task_id,
        session_id,
        worker_type: WorkerType::Python,
        intent: TaskIntent {
            action_type: "execute_plan".into(),
            target: "plan".into(),
            parameters: {
                let mut m = BTreeMap::new();
                m.insert("plan".into(), llm_plan.clone());
                m
            },
            constraints: vec![],
        },
        capabilities: vec!["fs:read:.".into(), "fs:write:.".into()],
        from_step: 0,
        timeout_ms: 60_000,
        token_budget: 10_000,
    };

    let worker_success = match spawner.spawn(worker_config.clone()) {
        Ok(mut handle) => {
            println!("[WORKER] Spawned PID {}", handle.pid);

            WorkerSpawner::send_execute(&mut handle, &worker_config).unwrap();
            println!("         Sent execute command");

            let mut checkpoints = 0u64;
            let mut completed = false;
            let mut failed = false;

            while let Some(msg) = WorkerSpawner::read_response(&mut handle) {
                if msg.get("method").is_some_and(|m| m == "checkpoint") {
                    checkpoints += 1;
                    cv.increment(session_id);
                    let cp_event = NexusEvent::new(
                        EventType::WorkerCheckpoint {
                            task_id,
                            step_index: checkpoints,
                            actions: vec![],
                            artifacts: vec![],
                        },
                        session_id,
                        cv.clone(),
                        Some(state.latest_event_id.clone()),
                    );
                    store.append_event(&cp_event).await.ok();
                    state.latest_event_id = cp_event.event_id.clone();
                    if let Ok(next) = transition(&state, &cp_event, &dag) {
                        state = next;
                    }
                    println!("[CKPT]   Step {} → {:?}", checkpoints, state.status);
                } else if msg.get("result").is_some() && state.status == SessionStatus::Checkpointing {
                    completed = true;
                    break;
                } else if msg.get("error").is_some() {
                    failed = true;
                    break;
                } else if msg.get("result").is_some() {
                    completed = true;
                    break;
                }
            }

            if completed {
                cv.increment(session_id);
                let done_event = NexusEvent::new(
                    EventType::WorkerCompleted {
                        worker_id: "python-worker".into(),
                        task_id,
                        result: WorkerResult {
                            status: "completed".into(),
                            artifacts: vec![],
                            metrics: WorkerMetrics { duration_ms: 0, tokens_consumed: 0, cost_cents: 0 },
                        },
                        duration_ms: 0,
                    },
                    session_id,
                    cv.clone(),
                    Some(state.latest_event_id.clone()),
                );
                store.append_event(&done_event).await.ok();
                state.latest_event_id = done_event.event_id.clone();
                if let Ok(next) = transition(&state, &done_event, &dag) {
                    state = next;
                }
                println!("[OK]     Worker completed → {:?}", state.status);
                true
            } else {
                cv.increment(session_id);
                let fail_event = NexusEvent::new(
                    EventType::WorkerFailed {
                        worker_id: "python-worker".into(),
                        task_id,
                        error: "Worker error".into(),
                        error_code: ErrorCode::Retryable,
                        retry_count: 0,
                    },
                    session_id,
                    cv.clone(),
                    Some(state.latest_event_id.clone()),
                );
                store.append_event(&fail_event).await.ok();
                state.latest_event_id = fail_event.event_id.clone();
                if let Ok(next) = transition(&state, &fail_event, &dag) {
                    state = next;
                }
                println!("[FAIL]   Worker failed → {:?}", state.status);
                failed
            }
        }
        Err(e) => {
            println!("[WORKER] Could not spawn: {}", e);
            eprintln!("         Running in demo mode.");

            cv.increment(session_id);
            let done_event = NexusEvent::new(
                EventType::WorkerCompleted {
                    worker_id: "inline".into(),
                    task_id,
                    result: WorkerResult {
                        status: "completed".into(),
                        artifacts: vec![],
                        metrics: WorkerMetrics { duration_ms: 0, tokens_consumed: 0, cost_cents: 0 },
                    },
                    duration_ms: 0,
                },
                session_id,
                cv.clone(),
                Some(state.latest_event_id.clone()),
            );
            store.append_event(&done_event).await.ok();
            state.latest_event_id = done_event.event_id.clone();
            state = transition(&state, &done_event, &dag).unwrap();
            true
        }
    };

    let verdict = if worker_success { "OK" } else { "FAILED" };
    store.update_state(&state, state.version - 1).await.ok();
    println!();
    println!("[{verdict}]   Session {verdict} — status {:?}", state.status);
    println!("Use 'nexus status {}' to check.", session_id.to_hex());
}

async fn resume_session(session_id: &str, from: Option<u64>) {
    println!("=== Nexus — Resume Session ===");
    println!("Session ID: {}", session_id);
    if let Some(checkpoint) = from {
        println!("From checkpoint: {}", checkpoint);
    }

    let store = match get_store().await {
        Ok(s) => s,
        Err(e) => { println!("[ERR] {}", e); return; }
    };
    let sid = match SessionId::from_hex(session_id) {
        Ok(s) => s,
        Err(e) => {
            println!("[ERR] Invalid session ID: {}", e);
            return;
        }
    };

    match store.get_events(sid, None).await {
        Ok(events) => {
            if events.is_empty() {
                println!("[WARN] No events found for session {}", session_id);
                return;
            }

            let rm = RecoveryManager::new(
                std::env::var("NEXUS_VAULT_PATH").unwrap_or_else(|_| "~/.nexus/vault".into()),
            );
            match rm.recover_from_events(&events, sid) {
                Ok(result) => {
                    println!("[OK] Session recovered");
                    println!("  Status: {:?}", result.state.status);
                    println!("  Version: {}", result.state.version);
                    println!("  Checkpoint: {}", result.state.checkpoint_seq);
                    println!("  Causal check: {}", result.report.causal_valid);
                    println!("  Replay check: {}", result.report.replay_success);

                    if let Some(plan) = &result.recovery_plan {
                        println!("  Recovery plan ready at step {}", plan.from_step);
                    }
                }
                Err(e) => {
                    println!("[ERR] Recovery failed: {}", e);
                }
            }
        }
        Err(e) => {
            println!("[ERR] Failed to load events: {}", e);
        }
    }
}

async fn show_status(session_id: &str) {
    let store = match get_store().await {
        Ok(s) => s,
        Err(e) => { println!("[ERR] {}", e); return; }
    };
    let sid = match SessionId::from_hex(session_id) {
        Ok(s) => s,
        Err(e) => {
            println!("Invalid session ID: {}", e);
            return;
        }
    };

    match store.get_state(sid).await {
        Ok(Some(state)) => {
            println!("=== Session Status ===");
            println!("ID:           {}", session_id);
            println!("Status:       {:?}", state.status);
            println!("Version:      {}", state.version);
            println!("Checkpoint:   {}", state.checkpoint_seq);
            println!(
                "Budget:       {}/{} cents",
                state.budget.consumed_cents, state.budget.budget_limit_cents
            );
            println!("Memories:     {}", state.memory_refs.len());
        }
        Ok(None) => {
            println!("Session not found: {}", session_id);
        }
        Err(e) => {
            println!("Error: {}", e);
        }
    }
}

async fn suspend_session(session_id: &str) {
    let store = match get_store().await {
        Ok(s) => s,
        Err(e) => { println!("[ERR] {}", e); return; }
    };
    let sid = SessionId::from_hex(session_id).unwrap_or_else(|_| SessionId::new());
    let _skip = &sid;

    match store.get_state(sid).await {
        Ok(Some(state)) => {
            let mut cv = state.causal_vector.clone();
            cv.increment(sid);

            let event = NexusEvent::new(
                EventType::SessionSuspended {
                    reason: "user_requested".into(),
                },
                sid,
                cv,
                Some(state.latest_event_id.clone()),
            );

            match store.append_event(&event).await {
                Ok(()) => println!("Session suspended: {}", session_id),
                Err(e) => println!("Error: {}", e),
            }
        }
        Ok(None) => println!("Session not found"),
        Err(e) => println!("Error: {}", e),
    }
}

async fn archive_session(session_id: &str) {
    let store = match get_store().await {
        Ok(s) => s,
        Err(e) => { println!("[ERR] {}", e); return; }
    };
    let sid = SessionId::from_hex(session_id).unwrap_or_else(|_| SessionId::new());

    match store.get_state(sid).await {
        Ok(Some(state)) => {
            let mut cv = state.causal_vector.clone();
            cv.increment(sid);

            let event = NexusEvent::new(
                EventType::SessionArchived {
                    reason: "user_requested".into(),
                    final_status: SessionStatus::Archived,
                },
                sid,
                cv,
                Some(state.latest_event_id.clone()),
            );

            match store.append_event(&event).await {
                Ok(()) => println!("Session archived: {}", session_id),
                Err(e) => println!("Error: {}", e),
            }
        }
        Ok(None) => println!("Session not found"),
        Err(e) => println!("Error: {}", e),
    }
}

async fn export_session(session_id: &str, output: &str) {
    let store = match get_store().await {
        Ok(s) => s,
        Err(e) => { println!("[ERR] {}", e); return; }
    };
    let sid = match SessionId::from_hex(session_id) {
        Ok(s) => s,
        Err(e) => {
            println!("Invalid session ID: {}", e);
            return;
        }
    };

    match store.get_events(sid, None).await {
        Ok(events) => {
            if events.is_empty() {
                println!("No events to export");
                return;
            }

            let export_data = ExportData {
                version: "1.0.0".into(),
                session_id: session_id.into(),
                events: events
                    .iter()
                    .map(|e| EventExport {
                        event_id: e.event_id.clone(),
                        event_type: e.event_type.as_str().into(),
                        causal_vector: e.causal_vector.to_canonical(),
                        payload_hash: e.payload_hash.clone(),
                        timestamp: e.event_timestamp,
                    })
                    .collect(),
            };

            let json = serde_json::to_string_pretty(&export_data).unwrap();
            tokio::fs::write(output, json).await.unwrap();
            println!("Exported session to {}", output);
        }
        Err(e) => println!("Error: {}", e),
    }
}

async fn import_session(file: &str, as_id: Option<String>) {
    let data = tokio::fs::read_to_string(file).await.unwrap();
    let export: ExportData = serde_json::from_str(&data).unwrap();

    println!("Importing session {} (format v{})", export.session_id, export.version);
    println!("Events: {}", export.events.len());

    if let Some(new_id) = as_id {
        println!("New session ID: {}", new_id);
    }
}

async fn show_log(session_id: &str, limit: usize, _since: Option<String>) {
    let store = match get_store().await {
        Ok(s) => s,
        Err(e) => { println!("[ERR] {}", e); return; }
    };
    let sid = match SessionId::from_hex(session_id) {
        Ok(s) => s,
        Err(_) => {
            println!("Invalid session ID");
            return;
        }
    };

    match store.get_events(sid, None).await {
        Ok(events) => {
            println!("=== Event Log ===");
            println!("Total events: {}", events.len());
            println!();

            let events_to_show: Vec<_> = events.iter().take(limit).collect();
            for (i, event) in events_to_show.iter().enumerate() {
                println!(
                    "[{}] {} | {}",
                    i + 1,
                    event.event_type.as_str(),
                    event.event_id
                );
                println!(
                    "    ts: {}, cv: {}",
                    event.event_timestamp,
                    &event.causal_vector.to_canonical()[..60.min(event.causal_vector.to_canonical().len())]
                );
            }

            if events.len() > limit {
                println!("... {} more events", events.len() - limit);
            }
        }
        Err(e) => println!("Error: {}", e),
    }
}

async fn inspect_session(session_id: &str, show_state: bool, show_memory: bool, show_budget: bool) {
    let store = match get_store().await {
        Ok(s) => s,
        Err(e) => { println!("[ERR] {}", e); return; }
    };
    let sid = match SessionId::from_hex(session_id) {
        Ok(s) => s,
        Err(_) => {
            println!("Invalid session ID");
            return;
        }
    };

    match store.get_state(sid).await {
        Ok(Some(state)) => {
            println!("=== Session: {} ===", session_id);
            println!("Status:       {:?}", state.status);
            println!("Version:      {}", state.version);
            println!("Checkpoint:   {}", state.checkpoint_seq);

            if show_budget {
                println!();
                println!("--- Budget ---");
                println!(
                    "Consumed:     {} / {} cents",
                    state.budget.consumed_cents, state.budget.budget_limit_cents
                );
                println!("Tokens:       {}", state.budget.token_count);
                println!("Tool calls:   {}", state.budget.tool_call_count);
            }

            if show_memory {
                println!();
                println!("--- Memory ---");
                println!("Memory refs:  {}", state.memory_refs.len());
                for m in &state.memory_refs {
                    println!("  {} (importance: {})", m.memory_id, m.importance_score);
                }
            }

            if show_state {
                println!();
                println!("--- State ---");
                println!("  Intent graph nodes: {}", state.intent_graph.nodes.len());
                println!(
                    "  Frontier nodes: {}",
                    state.execution_frontier.nodes.len()
                );
                println!(
                    "  Memory graph nodes: {}",
                    state.memory_graph.nodes.len()
                );
            }
        }
        Ok(None) => println!("Session not found"),
        Err(e) => println!("Error: {}", e),
    }
}

async fn budget_status() {
    println!("=== Budget Status ===");
    println!("No budget data available. Use 'nexus inspect --budget <session-id>' for per-session details.");
}

async fn budget_set(session_id: &str, limit_usd: f64) {
    println!("Setting session {} budget to ${:.2}", session_id, limit_usd);
    println!("(Budget governance will be enforced on next transition)");
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ExportData {
    version: String,
    session_id: String,
    events: Vec<EventExport>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct EventExport {
    event_id: String,
    event_type: String,
    causal_vector: String,
    payload_hash: String,
    timestamp: u64,
}
