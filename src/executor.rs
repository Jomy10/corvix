use std::any::Any;
use std::num::NonZero;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use log::*;

use crate::{Scheduler, SchedulerError, SchedulerInstance, TaskExecutionError};

pub struct Executor {
    num_workers: NonZero<usize>,
}

impl Executor {
    pub fn new(num_workers: Option<NonZero<usize>>) -> Self {
        let nproc = num_workers
            .unwrap_or(std::thread::available_parallelism().unwrap_or(NonZero::new(3usize).unwrap()));

        Executor {
            num_workers: nproc,
        }
    }

    pub fn run(&self, scheduler: Scheduler) -> Result<ExecutorFuture, ExecutorError> {
        // FIFO + Priority
        let task_queue = Arc::new(scheduler.create_instance()?);

        let handles = (0_usize..self.num_workers.get()).map(|_worker_id| {
            let task_queue = task_queue.clone();
            thread::spawn(move || {
                Self::worker_impl(task_queue)
            })
        }).collect::<Vec<_>>();

        return Ok(ExecutorFuture { worker_handles: handles })
    }

    fn worker_impl(task_queue: Arc<SchedulerInstance>) -> Result<(), ExecutorError> {
        loop {
            let Some(task_id) = task_queue.get_next_wait()? else {
                return Ok(());
            };

            trace!("Starting task {}", task_id);

            task_queue.execute_task(&task_id)?;

            trace!("Completed task {task_id}");
        }
    }
}

pub struct ExecutorFuture {
    worker_handles: Vec<JoinHandle<Result<(), ExecutorError>>>,
    // running: Arc<AtomicBool>,
}

impl ExecutorFuture {
    pub fn wait(self) -> Result<(), ExecutorError> {
        for handle in self.worker_handles {
            handle.join().map_err(|err| ExecutorError::JoinError(err))??;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub enum ExecutorError {
    JoinError(Box<dyn Any + Send>),
    SchedulerError(SchedulerError),
    TaskExecutionError(TaskExecutionError)
}

impl From<SchedulerError> for ExecutorError {
    fn from(value: SchedulerError) -> Self {
        Self::SchedulerError(value)
    }
}

impl From<TaskExecutionError> for ExecutorError {
    fn from(value: TaskExecutionError) -> Self {
        Self::TaskExecutionError(value)
    }
}

impl std::error::Error for ExecutorError {}

impl std::fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}", self))
    }
}
