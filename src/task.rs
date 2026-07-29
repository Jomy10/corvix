use std::any::Any;
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use uuid::Uuid;

pub type TaskId = Uuid;
type BoxedWork<Err> = Box<dyn FnOnce() -> Result<(), Err> + Send + Sync + 'static>;

pub(crate) struct Task<Err> {
    // TODO: for logging
    id: TaskId,
    #[allow(unused)]
    name: Option<String>,
    pub(crate) priority: u32,
    work: Mutex<Option<BoxedWork<Err>>>,
    pub(crate) failed: AtomicBool,
    pub(crate) can_start: Option<Arc<AtomicBool>>,
}

impl<Err> Task<Err> {
    pub(crate) fn new(work: BoxedWork<Err>, id: TaskId, priority: u32, name: Option<String>) -> Self {
        Self {
            id,
            name,
            priority,
            work: Mutex::new(Some(work)),
            failed: false.into(),
            can_start: None
        }
    }

    pub(crate) fn name(&self) -> Option<&str> {
        self.name.as_ref().map(|s| s.as_str())
    }

    pub(crate) fn run(&self) -> Result<(), TaskExecutionError<Err>> {
        let mut work = self.work.lock()
            .map_err(|_| TaskExecutionError::LockPoisonedError(self.id, self.name.clone()))?;
        if let Err(err) = (work.take().unwrap())() {
            self.failed.store(true, Ordering::Relaxed);
            return Err(TaskExecutionError::Err(self.id, self.name.clone(), err))
        }

        Ok(())
    }

    pub(crate) fn cancel(&self) {
        self.failed.store(true, Ordering::Relaxed);
    }

    pub(crate) fn id(&self) -> TaskId {
        self.id
    }

    pub(crate) fn set_conditional(&mut self, can_start: Arc<AtomicBool>) {
        self.can_start = Some(can_start);
    }
}

impl<Err> Debug for Task<Err> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Task")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("priority", &self.priority)
            .field("failed", &self.failed)
            .finish()
    }
}

#[derive(Debug)]
pub enum TaskExecutionError<Err> {
    LockPoisonedError(TaskId, Option<String>),
    /// Error thrown from the actual task
    Err(TaskId, Option<String>, Err),
    /// Task was cancelled because one of their dependencies failed or was also cancelled
    Cancelled(TaskId, Option<String>),
    Panic(Box<dyn Any + Send>)
}

impl<Err: std::fmt::Debug> std::error::Error for TaskExecutionError<Err> {}

impl<Err: std::fmt::Debug> std::fmt::Display for TaskExecutionError<Err> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}", self))
    }
}
