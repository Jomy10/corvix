use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};

use itertools::Itertools;
use petgraph::graphmap::DiGraphMap;
use uuid::Uuid;

use crate::{ExecutorError, Task, TaskId};

pub struct Scheduler {
    pub(crate) tasks: HashMap<TaskId, Task>,
    pub(crate) dag: DiGraphMap<Uuid, ()>,
}

impl Scheduler {
    pub fn new() -> Self {
        Self {
            tasks: HashMap::new(),
            dag: DiGraphMap::new()
        }
    }

    pub fn add_task(&mut self, work: impl FnOnce() + Send + Sync + 'static) -> Result<TaskId, SchedulerError> {
        return self.add_task_with_priority(work, 0);
    }

    pub fn add_task_with_priority(&mut self,
        work: impl FnOnce() + Send + Sync + 'static,
        priority: u32
    ) -> Result<TaskId, SchedulerError> {
        let id = Uuid::new_v4();

        let task = Task::new(Box::new(work), priority);

        self.tasks.insert(id, task);
        self.dag.add_node(id);

        return Ok(id);
    }

    pub fn add_dependency(&mut self, node: TaskId, depends_on: TaskId) {
        self.dag.add_edge(depends_on, node, ());
    }

    pub fn change_priority(&mut self, task_id: TaskId, priority: u32) {
        let task = self.tasks.get_mut(&task_id).unwrap();
        task.priority = priority;
    }

    pub(crate) fn create_instance(self) -> Result<SchedulerInstance, SchedulerError> {
        if petgraph::algo::is_cyclic_directed(&self.dag) {
            return Err(SchedulerError::CyclicalDependency);
        }

        return Ok(SchedulerInstance::new(self));
    }
}

#[derive(Debug)]
pub enum SchedulerError {
    PoisonError,
    CyclicalDependency,
}

impl<'a, T> From<PoisonError<MutexGuard<'a, T>>> for SchedulerError {
    fn from(_: PoisonError<MutexGuard<'a, T>>) -> Self {
        Self::PoisonError
    }
}

impl std::error::Error for SchedulerError {}

impl std::fmt::Display for SchedulerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}", self))
    }
}

pub(crate) struct SchedulerInstance {
    scheduler: Scheduler,
    // Priorities in descending order
    priorities: Vec<u32>,
    // Nodes which have all their dependencies resolved, grouped by priority
    accessible_nodes: Mutex<HashMap<u32, VecDeque<TaskId>>>,
    completed_nodes: Mutex<HashSet<TaskId>>,
    total_nodes: usize,
    count_completed: AtomicUsize,
    signal: Condvar,
}

impl SchedulerInstance {
    fn new(scheduler: Scheduler) -> Self {
        let priorities = scheduler.tasks.values()
            .map(|n| n.priority)
            .unique()
            .sorted_by(|a, b| Ord::cmp(b, a)) // sorted in descending order
            .collect::<Vec<u32>>();

        let mut accessible_nodes: HashMap<u32, VecDeque<Uuid>> = HashMap::new();
        for priority in &priorities {
            accessible_nodes.insert(*priority, VecDeque::new());
        }

        for node in scheduler.tasks.keys() {
            let priority = scheduler.tasks[node].priority;
            if scheduler.dag.neighbors_directed(*node, petgraph::Direction::Incoming).next() == None {
                accessible_nodes.get_mut(&priority).unwrap().push_front(*node);
            }
        }

        let total_nodes = scheduler.tasks.values().count();

        Self {
            scheduler: scheduler,
            priorities,
            accessible_nodes: Mutex::new(accessible_nodes),
            completed_nodes: Mutex::new(HashSet::new()),
            total_nodes,
            count_completed: 0.into(),
            signal: Condvar::new(),
        }
    }

    // Gets the next node that should be executed
    pub(crate) fn get_next_wait(&self) -> Result<Option<TaskId>, SchedulerError> {
        let mut lock = self.accessible_nodes.lock()?;
        loop {
            for priority in &self.priorities {
                let nodes = lock.get_mut(priority).unwrap();
                let node = nodes.pop_back();
                if node.is_some() {
                    return Ok(node);
                }
            }

            if self.total_nodes == self.count_completed.load(Ordering::Relaxed) {
                return Ok(None);
            }

            lock = self.signal.wait(lock)?;
        }
    }

    fn complete_task(&self, id: &TaskId) -> Result<(), ExecutorError> {
        let descendants = self.scheduler.dag.neighbors_directed(*id, petgraph::Direction::Outgoing);
        let nodes = descendants
            .map(|desc| (
                desc,
                &self.scheduler.tasks[&desc],
                self.scheduler.dag.neighbors_directed(desc, petgraph::Direction::Incoming)
            ));

        let mut completed_nodes = self.completed_nodes.lock().map_err(|err| SchedulerError::from(err))?;
        completed_nodes.insert(*id);

        let nodes = nodes
            .filter_map(|node| {
                let mut node = node;
                // Only nodes whose dependencies have completed
                if node.2.find(|dependency| !completed_nodes.contains(&dependency)) == None {
                    Some((node.0, node.1))
                } else {
                    None
                }
            })
            .map(|node| (node.0, node.1))
            .collect::<Vec<_>>();
        drop(completed_nodes);

        // Add newly available nodes to accessible_nodes
        let mut accessible_nodes = self.accessible_nodes.lock().map_err(|err| SchedulerError::from(err))?;
        let nodes_count = nodes.len();
        for (descendant, task) in nodes {
            accessible_nodes.get_mut(&task.priority)
                .unwrap()
                .push_back(descendant);
        }

        self.count_completed.fetch_add(1, Ordering::Relaxed);

        if nodes_count > 0 || self.count_completed.load(Ordering::Relaxed) == self.total_nodes {
            self.signal.notify_all();
        }

        Ok(())
    }

    pub(crate) fn execute_task(&self, id: &TaskId) -> Result<(), ExecutorError> {
        let task = &self.scheduler.tasks[&id];
        task.run()?;

        self.complete_task(id)?;

        Ok(())
    }
}
