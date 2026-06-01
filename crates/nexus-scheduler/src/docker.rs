use std::collections::BTreeMap;
use nexus_core::TaskId;
use crate::{SchedulerTask, CapabilityMode};

pub struct DockerScheduler {
    ready_queue: Vec<SchedulerTask>,
    lock_table: BTreeMap<String, TaskId>,
    max_concurrency: usize,
    active_workers: BTreeMap<TaskId, String>,
    #[allow(dead_code)]
    docker_socket: String,
}

impl DockerScheduler {
    pub fn new(max_concurrency: usize) -> Self {
        Self {
            ready_queue: Vec::new(),
            lock_table: BTreeMap::new(),
            max_concurrency,
            active_workers: BTreeMap::new(),
            docker_socket: "unix:///var/run/docker.sock".into(),
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
                        self.lock_table
                            .insert(cap.resource.clone(), task.task_id);
                    }
                }

                let container_name = format!("nexus-worker-{}", hex::encode(task.task_id.0));
                self.active_workers
                    .insert(task.task_id, container_name);
                dispatched.push(task.task_id);
            } else {
                self.ready_queue.insert(0, task);
                break;
            }
        }
        dispatched
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

    pub fn generate_docker_run_command(
        worker_image: &str,
        task_id: TaskId,
        capabilities: &[String],
    ) -> String {
        let caps_str = capabilities.join(",");
        format!(
            "docker run --rm --network none --read-only \
             --name nexus-worker-{} \
             -e NEXUS_WORKER_CAPABILITIES='{}' \
             {}",
            hex::encode(task_id.0),
            caps_str,
            worker_image
        )
    }

    pub fn pending_count(&self) -> usize {
        self.ready_queue.len()
    }

    pub fn active_count(&self) -> usize {
        self.active_workers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SchedulerTask, CapabilityMode, RequiredCapability};

    #[test]
    fn test_docker_scheduler_dispatches_tasks() {
        let mut sched = DockerScheduler::new(3);
        let t1 = TaskId::from_bytes([1u8; 16]);
        let t2 = TaskId::from_bytes([2u8; 16]);

        sched.enqueue(SchedulerTask {
            task_id: t1,
            required_capabilities: vec![],
            priority: 1,
        });
        sched.enqueue(SchedulerTask {
            task_id: t2,
            required_capabilities: vec![],
            priority: 1,
        });

        let dispatched = sched.tick();
        assert_eq!(dispatched.len(), 2);
        assert_eq!(sched.active_count(), 2);
    }

    #[test]
    fn test_docker_run_command_format() {
        let tid = TaskId::from_bytes([1u8; 16]);
        let cmd = DockerScheduler::generate_docker_run_command(
            "nexus/python-worker:v1.0",
            tid,
            &["fs:read:/src".into(), "fs:write:/src".into()],
        );
        assert!(cmd.contains("docker run"));
        assert!(cmd.contains("--network none"));
        assert!(cmd.contains("--read-only"));
        assert!(cmd.contains("nexus/python-worker:v1.0"));
    }
}
