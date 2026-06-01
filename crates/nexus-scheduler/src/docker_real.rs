/// Real Docker scheduler using the bollard crate.
/// Communicates with Docker daemon via Unix socket or TCP.
#[cfg(feature = "docker")]
use bollard::Docker;
#[cfg(feature = "docker")]
use bollard::container::{
    Config as DockerConfig, CreateContainerOptions, HostConfig,
    RemoveContainerOptions, StartContainerOptions,
};
#[cfg(feature = "docker")]
use bollard::models::{HostConfig as BollardHostConfig, PortBinding};
use std::collections::BTreeMap;
use nexus_core::TaskId;
use crate::{SchedulerTask, CapabilityMode};

#[derive(Debug, Clone)]
pub struct RealDockerScheduler {
    ready_queue: Vec<SchedulerTask>,
    lock_table: BTreeMap<String, TaskId>,
    max_concurrency: usize,
    active_workers: BTreeMap<TaskId, String>,
    #[cfg(feature = "docker")]
    client: Docker,
    #[allow(dead_code)]
    worker_image: String,
}

impl RealDockerScheduler {
    #[cfg(feature = "docker")]
    pub fn new(max_concurrency: usize, worker_image: String) -> Self {
        let client = Docker::connect_with_local_defaults()
            .expect("Failed to connect to Docker daemon");

        Self {
            ready_queue: Vec::new(),
            lock_table: BTreeMap::new(),
            max_concurrency,
            active_workers: BTreeMap::new(),
            client,
            worker_image,
        }
    }

    #[cfg(not(feature = "docker"))]
    pub fn new(max_concurrency: usize, worker_image: String) -> Self {
        tracing::warn!("Docker feature not enabled; using stub scheduler");
        Self {
            ready_queue: Vec::new(),
            lock_table: BTreeMap::new(),
            max_concurrency,
            active_workers: BTreeMap::new(),
            worker_image,
        }
    }

    pub fn enqueue(&mut self, task: SchedulerTask) {
        self.ready_queue.push(task);
    }

    pub fn tick(&mut self) -> Vec<TaskId> {
        let mut dispatched = Vec::new();
        while dispatched.len() < self.max_concurrency
            && self.active_workers.len() < self.max_concurrency
        {
            let Some(task) = self.ready_queue.pop() else { break };
            let can_dispatch = task.required_capabilities.iter().all(|cap| match cap.mode {
                CapabilityMode::Exclusive => !self.lock_table.contains_key(&cap.resource),
                CapabilityMode::Shared => true,
            });

            if can_dispatch {
                for cap in &task.required_capabilities {
                    if cap.mode == CapabilityMode::Exclusive {
                        self.lock_table.insert(cap.resource.clone(), task.task_id);
                    }
                }
                let container_name = format!("nexus-worker-{}", hex::encode(&task.task_id.0[..8]));
                self.active_workers.insert(task.task_id, container_name);
                dispatched.push(task.task_id);
            } else {
                self.ready_queue.insert(0, task);
                break;
            }
        }
        dispatched
    }

    #[cfg(feature = "docker")]
    pub async fn start_container(
        &self,
        task_id: TaskId,
        capabilities: &[String],
    ) -> Result<String, String> {
        let container_name = format!("nexus-worker-{}", hex::encode(&task_id.0[..8]));

        let env_vars: Vec<String> = capabilities
            .iter()
            .map(|c| format!("NEXUS_CAPABILITY={}", c))
            .chain(std::iter::once(format!("NEXUS_TASK_ID={}", hex::encode(task_id.0))))
            .collect();

        let config = DockerConfig {
            image: Some(self.worker_image.clone()),
            env: Some(env_vars),
            host_config: Some(HostConfig {
                network_mode: Some("none".into()),
                readonly_rootfs: Some(true),
                auto_remove: Some(true),
                ..Default::default()
            }),
            ..Default::default()
        };

        let options = CreateContainerOptions {
            name: container_name.clone(),
            platform: None,
        };

        self.client
            .create_container(Some(options), config)
            .await
            .map_err(|e| format!("create container: {}", e))?;

        self.client
            .start_container(&container_name, None::<StartContainerOptions<String>>)
            .await
            .map_err(|e| format!("start container: {}", e))?;

        tracing::info!(
            target = "nexus.scheduler.docker",
            container = %container_name,
            task = %hex::encode(&task_id.0[..8]),
            "Container started"
        );

        Ok(container_name)
    }

    #[cfg(feature = "docker")]
    pub async fn stop_container(&self, task_id: TaskId) -> Result<(), String> {
        let container_name = format!("nexus-worker-{}", hex::encode(&task_id.0[..8]));

        let options = RemoveContainerOptions {
            force: true,
            ..Default::default()
        };

        self.client
            .remove_container(&container_name, Some(options))
            .await
            .map_err(|e| format!("remove container: {}", e))?;

        Ok(())
    }

    #[cfg(not(feature = "docker"))]
    pub async fn start_container(
        &self,
        _task_id: TaskId,
        _capabilities: &[String],
    ) -> Result<String, String> {
        Ok("stub-container".into())
    }

    #[cfg(not(feature = "docker"))]
    pub async fn stop_container(&self, _task_id: TaskId) -> Result<(), String> {
        Ok(())
    }

    pub fn release_task(&mut self, task_id: TaskId) {
        let to_remove: Vec<String> = self
            .lock_table
            .iter()
            .filter(|(_, owner)| **owner == task_id)
            .map(|(resource, _)| resource.clone())
            .collect();
        for resource in to_remove {
            self.lock_table.remove(&resource);
        }
        self.active_workers.remove(&task_id);
    }

    pub fn pending_count(&self) -> usize { self.ready_queue.len() }
    pub fn active_count(&self) -> usize { self.active_workers.len() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_real_docker_scheduler_dispatches() {
        let mut sched = RealDockerScheduler::new(3, "nexus/worker:v1".into());
        let t1 = TaskId::from_bytes([1u8; 16]);
        sched.enqueue(SchedulerTask {
            task_id: t1,
            required_capabilities: vec![],
            priority: 1,
        });
        let dispatched = sched.tick();
        assert_eq!(dispatched.len(), 1);
    }
}
