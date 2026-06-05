#![deny(clippy::disallowed_types)]

use nexus_core::TaskId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityMode {
    Exclusive,
    Shared,
}

#[derive(Debug, Clone)]
pub struct RequiredCapability {
    pub resource: String,
    pub mode: CapabilityMode,
}

#[derive(Debug, Clone)]
pub struct SchedulerTask {
    pub task_id: TaskId,
    pub required_capabilities: Vec<RequiredCapability>,
    pub priority: u32,
}

pub mod local;
pub mod docker;
pub mod k8s;
pub mod k8s_real;

pub use local::LocalScheduler;
pub use docker::DockerScheduler;
pub use k8s::{K8sScheduler, K8sWorkerConfig, K8sPodSpec, K8sPodInfo, PodPhase};
pub use k8s_real::RealK8sScheduler;
