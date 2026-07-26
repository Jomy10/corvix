use std::sync::{Mutex, PoisonError};

use uuid::Uuid;

pub type TaskId = Uuid;
type BoxedWork = Box<dyn FnOnce() + Send + Sync + 'static>;

pub struct Task {
    // TODO: for logging
    #[allow(unused)]
    name: Option<String>,
    pub(crate) priority: u32,
    work: Mutex<Option<BoxedWork>>,
}

impl Task {
    pub(crate) fn new(work: BoxedWork, priority: u32) -> Self {
        Self {
            name: None,
            priority,
            work: Mutex::new(Some(work)),
        }
    }

    pub(crate) fn run(&self) -> Result<(), TaskExecutionError> {
        let mut work = self.work.lock()?;
        (work.take().unwrap())();

        Ok(())
    }
}

#[derive(Debug)]
pub enum TaskExecutionError {
    LockPoisonedError
}

impl<T> From<PoisonError<T>> for TaskExecutionError {
    fn from(_: PoisonError<T>) -> Self {
        Self::LockPoisonedError
    }
}

impl std::error::Error for TaskExecutionError {}

impl std::fmt::Display for TaskExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}", self))
    }
}

// #[derive(Clone)]
// pub struct TaskHandle {
//     pub(crate) id: TaskId,
//     pub(crate) state: Arc<Mutex<Option<SharedState>>>
// }

// impl TaskHandle {
//     pub fn precede(&self, other: &TaskHandle) -> Result<(), SchedulerError> {
//         let mut state = self.state.lock()?;
//         state.add_dependency(self.id, other.id);
//         Ok(())
//     }

//     pub fn succeed(&self, other: &TaskHandle) -> Result<(), SchedulerError> {
//         let mut state = self.state.lock()?;
//         state.add_dependency(other.id, self.id);
//         Ok(())
//     }

//     pub fn set_priority(&self, priority: u32) -> Result<(), SchedulerError> {
//         let mut state = self.state.lock()?;
//         state.tasks.get_mut(&self.id).unwrap().priority = priority;
//         Ok(())
//     }

//     pub fn priority(&self) -> Result<u32, SchedulerError> {
//         let state = self.state.lock()?;
//         Ok(state.tasks[&self.id].priority)
//     }

//     pub fn set_name(&self, name: impl Into<String>) -> Result<(), SchedulerError> {
//         let mut state = self.state.lock()?;
//         state.tasks.get_mut(&self.id).unwrap().name = Some(name.into());
//         Ok(())
//     }

//     pub fn name(&self) -> Result<Option<String>, SchedulerError> {
//         let state = self.state.lock()?;
//         Ok(state.tasks[&self.id].name.clone())
//     }
// }

// impl fmt::Debug for TaskHandle {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         f.debug_struct("TaskHandle").field("id", &self.id).finish()
//     }
// }
