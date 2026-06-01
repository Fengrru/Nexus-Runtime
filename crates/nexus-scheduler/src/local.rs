use std::collections::{BTreeMap, VecDeque};
use nexus_core::TaskId;
use tokio::sync::Mutex;
use std::sync::Arc;
use crate::{SchedulerTask, CapabilityMode};

pub struct LocalScheduler {
    ready_queue: VecDeque<SchedulerTask>,
    lock_table: Arc<Mutex<BTreeMap<String, TaskId>>>,
    max_concurrency: usize,
    active_workers: BTreeMap<TaskId, ()>,
}

impl LocalScheduler {
    pub fn new(max_concurrency: usize) -> Self {
        Self {
            ready_queue: VecDeque::new(),
            lock_table: Arc::new(Mutex::new(BTreeMap::new())),
            max_concurrency,
            active_workers: BTreeMap::new(),
        }
    }

    pub fn enqueue(&mut self, task: SchedulerTask) {
        self.ready_queue.push_back(task);
    }

    pub fn enqueue_batch(&mut self, tasks: Vec<SchedulerTask>) {
        for task in tasks {
            self.ready_queue.push_back(task);
        }
    }

    pub async fn tick(&mut self) -> Vec<TaskId> {
        let mut dispatched = Vec::new();
        let mut locks = self.lock_table.lock().await;

        while dispatched.len() < self.max_concurrency
            && self.active_workers.len() < self.max_concurrency
        {
            let Some(task) = self.ready_queue.pop_front() else {
                break;
            };

            let can_dispatch = task.required_capabilities.iter().all(|cap| match cap.mode {
                CapabilityMode::Exclusive => !locks.contains_key(&cap.resource),
                CapabilityMode::Shared => true,
            });

            if can_dispatch {
                for cap in &task.required_capabilities {
                    if cap.mode == CapabilityMode::Exclusive {
                        locks.insert(cap.resource.clone(), task.task_id);
                    }
                }
                self.active_workers.insert(task.task_id, ());
                dispatched.push(task.task_id);
            } else {
                self.ready_queue.push_back(task);
                break;
            }
        }

        dispatched
    }

    pub async fn release_task(&mut self, task_id: TaskId) {
        let mut locks = self.lock_table.lock().await;
        let to_remove: Vec<String> = locks
            .iter()
            .filter(|(_, owner)| **owner == task_id)
            .map(|(resource, _)| resource.clone())
            .collect();
        for resource in to_remove {
            locks.remove(&resource);
        }
        self.active_workers.remove(&task_id);
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
    fn test_scheduler_dispatches_tasks() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut sched = LocalScheduler::new(3);
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

            let dispatched = sched.tick().await;
            assert_eq!(dispatched.len(), 2);
            assert_eq!(sched.active_count(), 2);
        });
    }

    #[test]
    fn test_scheduler_respects_exclusive_locks() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut sched = LocalScheduler::new(3);
            let t1 = TaskId::from_bytes([1u8; 16]);
            let t2 = TaskId::from_bytes([2u8; 16]);

            sched.enqueue(SchedulerTask {
                task_id: t1,
                required_capabilities: vec![RequiredCapability {
                    resource: "file_a".into(),
                    mode: CapabilityMode::Exclusive,
                }],
                priority: 1,
            });
            sched.enqueue(SchedulerTask {
                task_id: t2,
                required_capabilities: vec![RequiredCapability {
                    resource: "file_a".into(),
                    mode: CapabilityMode::Exclusive,
                }],
                priority: 1,
            });

            let dispatched = sched.tick().await;
            assert_eq!(dispatched.len(), 1);
            assert_eq!(sched.pending_count(), 1);
        });
    }

    #[test]
    fn test_release_task_unlocks_resource() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut sched = LocalScheduler::new(3);
            let t1 = TaskId::from_bytes([1u8; 16]);
            let t2 = TaskId::from_bytes([2u8; 16]);

            sched.enqueue(SchedulerTask {
                task_id: t1,
                required_capabilities: vec![RequiredCapability {
                    resource: "file_a".into(),
                    mode: CapabilityMode::Exclusive,
                }],
                priority: 1,
            });
            sched.enqueue(SchedulerTask {
                task_id: t2,
                required_capabilities: vec![RequiredCapability {
                    resource: "file_a".into(),
                    mode: CapabilityMode::Exclusive,
                }],
                priority: 1,
            });

            let dispatched = sched.tick().await;
            assert_eq!(dispatched.len(), 1);

            sched.release_task(t1).await;

            let dispatched2 = sched.tick().await;
            assert_eq!(dispatched2.len(), 1);
        });
    }
}
