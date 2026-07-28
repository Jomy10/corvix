use std::borrow::Cow;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};

use itertools::Itertools;
use petgraph::graph::DiGraph;
use petgraph::graphmap::DiGraphMap;
use uuid::Uuid;

use crate::{ExecutionError, Task, TaskExecutionError, TaskId};

pub struct Scheduler<Err> {
    pub(crate) tasks: HashMap<TaskId, Task<Err>>,
    pub(crate) dag: DiGraphMap<Uuid, ()>,
}

impl<Err> Scheduler<Err> {
    /// The scheduler is in charge of determining which task the `Executor` should
    /// execute next. Tasks can be submitted using `add_task*` functions. Dependencies
    /// between tasks can be added using `add_dependency`.
    pub fn new() -> Self {
        Self {
            tasks: HashMap::new(),
            dag: DiGraphMap::new()
        }
    }

    pub fn add_task(&mut self, work: impl FnOnce() -> Result<(), Err> + Send + Sync + 'static) -> Result<TaskId, SchedulerError> {
        return self.add_task_impl(work, None, 0);
    }

    pub fn add_task_with_priority(&mut self,
        work: impl FnOnce() -> Result<(), Err> + Send + Sync + 'static,
        priority: u32
    ) -> Result<TaskId, SchedulerError> {
        self.add_task_impl(work, None, priority)
    }

    pub fn add_task_with_name(&mut self,
        work: impl FnOnce() -> Result<(), Err> + Send + Sync + 'static,
        name: impl Into<String>,
    ) -> Result<TaskId, SchedulerError> {
        self.add_task_impl(work, Some(name.into()), 0)
    }

    pub fn add_task_with_name_and_priority(&mut self,
        work: impl FnOnce() -> Result<(), Err> + Send + Sync + 'static,
        name: impl Into<String>,
        priority: u32
    ) -> Result<TaskId, SchedulerError> {
        self.add_task_impl(work, Some(name.into()), priority)
    }

    fn add_task_impl(&mut self,
        work: impl FnOnce() -> Result<(), Err> + Send + Sync + 'static,
        name: Option<String>,
        priority: u32
    ) -> Result<TaskId, SchedulerError> {
        let id = Uuid::new_v4();

        let task = Task::new(Box::new(work), id, priority, name);

        self.tasks.insert(id, task);
        self.dag.add_node(id);

        return Ok(id);
    }

    /// Add a dependency to the scheduler.
    ///
    /// Tasks depending on other tasks will not execute before all of their dependencies
    /// have finished executing.
    pub fn add_dependency(&mut self, node: TaskId, depends_on: TaskId) {
        self.dag.add_edge(depends_on, node, ());
    }

    /// Change the priority of a task. Tasks with a higher priority will be scheduled
    /// before tasks with a lower priority
    pub fn change_priority(&mut self, task_id: TaskId, priority: u32) {
        let task = self.tasks.get_mut(&task_id).unwrap();
        task.priority = priority;
    }

    /// Get the DAG of the scheduler. Can be used with `petgraph::dot::Dot` for
    /// visualization.
    pub fn graph(&self) -> &DiGraphMap<Uuid, ()> {
        &self.dag
    }

    /// Creates a new graph with names instead of id's, which is more suited
    /// for visualization using `petgraph::dot::Dot`.
    ///
    /// When feature `rayon` is enabled, this function will iterate over the
    /// nodes and edges in parallel, speeding up computation of the new graph.
    pub fn graph_with_names(&self) -> DiGraph<Cow<'_, str>, ()> {
        #[cfg(feature = "rayon")]
        {
            use std::sync::Arc;

            use petgraph::graph::NodeIndex;
            use rayon::iter::ParallelIterator;

            struct Storage<'a> {
                output_graph: Option<DiGraph<Cow<'a, str>, ()>>,
                indices: HashMap<Uuid, NodeIndex>,
            }

            let storage = Arc::new(Mutex::new(Storage {
                output_graph: Some(DiGraph::<Cow<'_, str>, ()>::new()),
                indices: HashMap::new()
            }));

            self.dag.par_nodes().for_each(|task_id| {
                let task = &self.tasks[&task_id];
                let name = task.name().map(|str| Cow::Borrowed(str))
                    .unwrap_or_else(|| Cow::Owned(task.id().to_string()));
                let mut storage = storage.lock().unwrap();
                let index = storage.output_graph.as_mut().unwrap().add_node(name);
                storage.indices.insert(task_id, index);
            });

            self.dag.par_all_edges().for_each(|edge| {
                let task_id1 = &self.tasks[&edge.0].id();
                let task_id2 = &self.tasks[&edge.1].id();
                let mut storage = storage.lock().unwrap();
                let index1 = storage.indices[task_id1];
                let index2 = storage.indices[task_id2];
                storage.output_graph.as_mut().unwrap().add_edge(index1, index2, *edge.2);
            });

            let mut lock = storage.lock().unwrap();
            return lock.output_graph.take().unwrap();
        }

        #[cfg(not(feature = "rayon"))]
        {
            let mut output_graph = DiGraph::<Cow<'_, str>, ()>::new();
            let mut indices = HashMap::new();

            for task_id in self.dag.nodes() {
                let task = &self.tasks[&task_id];
                let name = task.name().map(|str| Cow::Borrowed(str))
                    .unwrap_or_else(|| Cow::Owned(task.id().to_string()));
                let index = output_graph.add_node(name);
                indices.insert(task_id, index);
            }

            for edge in self.dag.all_edges() {
                let task_id1 = &self.tasks[&edge.0].id();
                let task_id2 = &self.tasks[&edge.1].id();
                output_graph.add_edge(indices[task_id1], indices[task_id2], *edge.2);
            }

            return output_graph;
        }
    }

    /// Consumes the `Scheduler` and makes it ready for execution
    pub(crate) fn create_instance(self, fail_fast: bool) -> Result<SchedulerInstance<Err>, SchedulerError> {
        if petgraph::algo::is_cyclic_directed(&self.dag) {
            return Err(SchedulerError::CyclicalDependency);
        }

        return Ok(SchedulerInstance::new(self, fail_fast));
    }
}

#[derive(Debug)]
pub enum SchedulerError {
    /// A lock failed due to a `std::sync::poison::PoisonError`
    PoisonError,
    /// A cyclical dependency was found in the scheduler
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

pub(crate) struct SchedulerInstance<Err> {
    scheduler: Scheduler<Err>,
    // Priorities in descending order
    priorities: Vec<u32>,
    // Nodes which have all their dependencies resolved, grouped by priority
    accessible_nodes: Mutex<HashMap<u32, VecDeque<TaskId>>>,
    completed_nodes: Mutex<HashSet<TaskId>>,
    total_nodes: usize,
    count_completed: AtomicUsize,
    signal: Condvar,
    fail_fast: bool
}

impl<Err> SchedulerInstance<Err> {
    fn new(scheduler: Scheduler<Err>, fail_fast: bool) -> Self {
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
            fail_fast
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

    fn complete_task(&self, id: &TaskId) -> Result<(), SchedulerError> {
        let descendants = self.scheduler.dag.neighbors_directed(*id, petgraph::Direction::Outgoing);
        let nodes = descendants
            .map(|desc| (
                desc,
                &self.scheduler.tasks[&desc],
                self.scheduler.dag.neighbors_directed(desc, petgraph::Direction::Incoming)
            ));

        let mut completed_nodes = self.completed_nodes.lock()?;
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
        let mut accessible_nodes = self.accessible_nodes.lock()?;
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

    fn execute_task_impl(&self, id: &TaskId) -> Result<(), ExecutionError<Err>> {
        let should_cancel = self.scheduler.dag
            .neighbors_directed(*id, petgraph::Direction::Incoming)
            .find(|dep| self.scheduler.tasks[dep].failed.load(Ordering::Relaxed))
            .is_some();
        let task = &self.scheduler.tasks[&id];

        if should_cancel {
            trace!("Task {id} cancelled");
            if self.fail_fast {
                return Ok(());
            } else {
                task.cancel();
                return Err(TaskExecutionError::Cancelled(task.id()).into())
            }
        }

        task.run()?;

        Ok(())
    }

    pub(crate) fn execute_task(&self, id: &TaskId) -> Result<(), ExecutionError<Err>> {
        if let Err(err) = self.execute_task_impl(id) {
            self.complete_task(id)?;
            return Err(err);
        } else {
            self.complete_task(id)?;
        }

        Ok(())
    }
}
