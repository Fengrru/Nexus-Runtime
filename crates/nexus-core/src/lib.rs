#![deny(clippy::disallowed_types)]

pub mod checkpoint;
pub mod effects;
pub mod entropy;
pub mod event;
pub mod export;
pub mod llm_proxy;
pub mod memory;
pub mod migration;
pub mod protocol;
pub mod recovery;
pub mod state_machine;
pub mod types;
pub mod vault;
pub mod wasm_worker;
pub mod worker_spawner;

pub use checkpoint::*;
pub use effects::*;
pub use entropy::*;
pub use event::*;
pub use export::SessionExport;
pub use llm_proxy::{LlmProxy, LlmRequest, LlmResponse, ProxyError};
pub use migration::{CrossNodeSession, MigrationStatus, SessionMigrationManager};
pub use protocol::*;
pub use recovery::*;
pub use state_machine::*;
pub use types::*;
pub use vault::{ContentVault, VaultEntry, VaultError};
pub use wasm_worker::{
    SandboxViolation, WasmInput, WasmOutput, WasmSandboxWorker, WasmSkill, WasmSkillRegistry,
};
pub use worker_spawner::{
    WorkerConfig as SpawnerConfig, WorkerHandle, WorkerSpawner, WorkerStatus,
};

#[cfg(test)]
mod golden_tests;
