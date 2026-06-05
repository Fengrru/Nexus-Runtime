use crate::{CapabilityMode, SchedulerTask};
use nexus_core::TaskId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct K8sWorkerConfig {
    pub namespace: String,
    pub worker_image: String,
    pub cpu_request: String,
    pub cpu_limit: String,
    pub memory_request: String,
    pub memory_limit: String,
    pub service_account: String,
    pub node_selector: BTreeMap<String, String>,
}

impl Default for K8sWorkerConfig {
    fn default() -> Self {
        Self {
            namespace: "nexus-workers".into(),
            worker_image: "nexus-runtime/python-worker:v1.0.0".into(),
            cpu_request: "250m".into(),
            cpu_limit: "500m".into(),
            memory_request: "256Mi".into(),
            memory_limit: "512Mi".into(),
            service_account: "nexus-worker".into(),
            node_selector: BTreeMap::new(),
        }
    }
}

pub struct K8sScheduler {
    ready_queue: Vec<SchedulerTask>,
    lock_table: BTreeMap<String, TaskId>,
    max_concurrency: usize,
    active_workers: BTreeMap<TaskId, K8sPodInfo>,
    config: K8sWorkerConfig,
}

#[derive(Debug, Clone)]
pub struct K8sPodInfo {
    pub pod_name: String,
    pub namespace: String,
    pub status: PodPhase,
    pub started_at: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PodPhase {
    Pending,
    Running,
    Succeeded,
    Failed,
    Unknown,
}

impl K8sScheduler {
    pub fn new(max_concurrency: usize, config: K8sWorkerConfig) -> Self {
        Self {
            ready_queue: Vec::new(),
            lock_table: BTreeMap::new(),
            max_concurrency,
            active_workers: BTreeMap::new(),
            config,
        }
    }

    pub fn enqueue(&mut self, task: SchedulerTask) {
        self.ready_queue.push(task);
    }

    pub fn tick(&mut self) -> Vec<(TaskId, K8sPodSpec)> {
        let mut dispatched = Vec::new();

        while dispatched.len() < self.max_concurrency
            && self.active_workers.len() < self.max_concurrency
        {
            let Some(task) = self.ready_queue.pop() else {
                break;
            };

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

                let pod_spec = self.generate_pod_spec(&task);
                let pod_name = format!("nexus-worker-{}", hex::encode(&task.task_id.0[..8]));

                self.active_workers.insert(
                    task.task_id,
                    K8sPodInfo {
                        pod_name: pod_name.clone(),
                        namespace: self.config.namespace.clone(),
                        status: PodPhase::Pending,
                        started_at: nexus_core::now_millis(),
                    },
                );

                dispatched.push((task.task_id, pod_spec));
            } else {
                self.ready_queue.insert(0, task);
                break;
            }
        }
        dispatched
    }

    fn generate_pod_spec(&self, task: &SchedulerTask) -> K8sPodSpec {
        let capabilities: Vec<String> = task
            .required_capabilities
            .iter()
            .map(|c| {
                format!(
                    "{}:{}",
                    if c.mode == CapabilityMode::Exclusive {
                        "exclusive"
                    } else {
                        "shared"
                    },
                    c.resource
                )
            })
            .collect();

        K8sPodSpec {
            pod_name: format!("nexus-worker-{}", hex::encode(&task.task_id.0[..8])),
            namespace: self.config.namespace.clone(),
            worker_image: self.config.worker_image.clone(),
            cpu_request: self.config.cpu_request.clone(),
            cpu_limit: self.config.cpu_limit.clone(),
            memory_request: self.config.memory_request.clone(),
            memory_limit: self.config.memory_limit.clone(),
            service_account: self.config.service_account.clone(),
            node_selector: self.config.node_selector.clone(),
            capabilities,
            task_id_hex: hex::encode(task.task_id.0),
            task_priority: task.priority,
        }
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

    pub fn update_pod_status(&mut self, task_id: TaskId, status: PodPhase) {
        if let Some(info) = self.active_workers.get_mut(&task_id) {
            info.status = status;
        }
    }

    pub fn pending_count(&self) -> usize {
        self.ready_queue.len()
    }

    pub fn active_count(&self) -> usize {
        self.active_workers.len()
    }

    pub fn workers_by_status(&self, status: PodPhase) -> usize {
        self.active_workers
            .values()
            .filter(|info| info.status == status)
            .count()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct K8sPodSpec {
    pub pod_name: String,
    pub namespace: String,
    pub worker_image: String,
    pub cpu_request: String,
    pub cpu_limit: String,
    pub memory_request: String,
    pub memory_limit: String,
    pub service_account: String,
    pub node_selector: BTreeMap<String, String>,
    pub capabilities: Vec<String>,
    pub task_id_hex: String,
    pub task_priority: u32,
}

impl K8sPodSpec {
    pub fn to_yaml(&self) -> String {
        let node_selector_yaml: String = if self.node_selector.is_empty() {
            String::new()
        } else {
            let entries: Vec<String> = self
                .node_selector
                .iter()
                .map(|(k, v)| format!("      {}: \"{}\"", k, v))
                .collect();
            format!("      nodeSelector:\n{}", entries.join("\n"))
        };

        let capabilities_env: String = self.capabilities.join(",");

        format!(
            r#"---
apiVersion: v1
kind: Pod
metadata:
  name: {pod_name}
  namespace: {namespace}
  labels:
    app: nexus-worker
    task-id: {task_id}
    priority: "{priority}"
spec:
  serviceAccountName: {service_account}
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: 1000
    readOnlyRootFilesystem: true
  containers:
  - name: worker
    image: {image}
    resources:
      requests:
        cpu: "{cpu_req}"
        memory: "{mem_req}"
      limits:
        cpu: "{cpu_lim}"
        memory: "{mem_lim}"
    env:
    - name: NEXUS_WORKER_CAPABILITIES
      value: "{caps}"
    - name: NEXUS_TASK_ID
      value: "{task_id}"
    securityContext:
      allowPrivilegeEscalation: false
      capabilities:
        drop:
        - ALL
{node_selector_section}
"#,
            pod_name = self.pod_name,
            namespace = self.namespace,
            task_id = self.task_id_hex,
            priority = self.task_priority,
            service_account = self.service_account,
            image = self.worker_image,
            cpu_req = self.cpu_request,
            cpu_lim = self.cpu_limit,
            mem_req = self.memory_request,
            mem_lim = self.memory_limit,
            caps = capabilities_env,
            node_selector_section = node_selector_yaml,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RequiredCapability;

    #[test]
    fn test_k8s_scheduler_dispatches_tasks() {
        let mut sched = K8sScheduler::new(3, K8sWorkerConfig::default());
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
        assert_eq!(sched.workers_by_status(PodPhase::Pending), 2);
    }

    #[test]
    fn test_pod_spec_yaml_generation() {
        let spec = K8sPodSpec {
            pod_name: "nexus-worker-abcdef01".into(),
            namespace: "nexus-workers".into(),
            worker_image: "nexus/python-worker:v1.0".into(),
            cpu_request: "250m".into(),
            cpu_limit: "500m".into(),
            memory_request: "256Mi".into(),
            memory_limit: "512Mi".into(),
            service_account: "nexus-worker".into(),
            node_selector: {
                let mut m = BTreeMap::new();
                m.insert("pool".into(), "worker".into());
                m
            },
            capabilities: vec!["fs:read:/src".into()],
            task_id_hex: "01010101010101010101010101010101".into(),
            task_priority: 1,
        };

        let yaml = spec.to_yaml();
        assert!(yaml.contains("apiVersion: v1"));
        assert!(yaml.contains("kind: Pod"));
        assert!(yaml.contains("nexus-worker-abcdef01"));
        assert!(yaml.contains("readOnlyRootFilesystem: true"));
        assert!(yaml.contains("pool: \"worker\""));
        assert!(yaml.contains("runAsNonRoot: true"));
        assert!(yaml.contains("drop:\n        - ALL"));
    }

    #[test]
    fn test_k8s_scheduler_release_and_reuse() {
        let mut sched = K8sScheduler::new(2, K8sWorkerConfig::default());
        let t1 = TaskId::from_bytes([1u8; 16]);

        sched.enqueue(SchedulerTask {
            task_id: t1,
            required_capabilities: vec![RequiredCapability {
                resource: "lock_a".into(),
                mode: CapabilityMode::Exclusive,
            }],
            priority: 1,
        });

        let dispatched = sched.tick();
        assert_eq!(dispatched.len(), 1);

        sched.release_task(t1);
        assert_eq!(sched.active_count(), 0);
    }
}
