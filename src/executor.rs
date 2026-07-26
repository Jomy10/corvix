use std::any::Any;
use std::num::NonZero;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
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

    /// Run all the submitted tasks in `scheduler`. When an error occurrs,
    /// the dependants of the node will be cancelled, while the other tasks will continue.
    pub fn run<Err: Send + 'static>(&self, scheduler: Scheduler<Err>) -> Result<ExecutorFuture<Err>, SchedulerError> {
        let task_queue = Arc::new(scheduler.create_instance(false)?);
        let errs = Arc::new(Mutex::new(Vec::new()));
        let cancel = Arc::new(AtomicBool::new(false));

        let handles = (0_usize..self.num_workers.get()).map(|_worker_id| {
            let task_queue = task_queue.clone();
            let errs = errs.clone();
            let cancel = cancel.clone();
            thread::spawn(move || {
                loop {
                    match Self::worker_impl(&task_queue, &cancel) {
                        Ok(()) => return Ok(()),
                        Err(err) => match err {
                            ExecutionError::SchedulerError(scheduler_error) => {
                                cancel.store(true, Ordering::Relaxed);
                                return Err(scheduler_error)
                            },
                            ExecutionError::TaskExecutionError(task_execution_error) => {
                                let mut errs = errs.lock()?;
                                errs.push(task_execution_error);
                            },
                        },
                    }
                }
            })
        }).collect::<Vec<_>>();

        Ok(ExecutorFuture { worker_handles: handles, errs: errs.clone() })
    }

    /// Run all submitted tasks in `scheduler`. When an error occurs,
    /// all tasks will be cancelled.
    pub fn run_fail_fast<Err: Send + 'static>(&self, scheduler: Scheduler<Err>) -> Result<ExecutorFailFastFuture<Err>, SchedulerError> {
        // FIFO + Priority
        let task_queue = Arc::new(scheduler.create_instance(true)?);
        let cancel = Arc::new(AtomicBool::new(false));

        let handles = (0_usize..self.num_workers.get()).map(|_worker_id| {
            let task_queue = task_queue.clone();
            let cancel = cancel.clone();
            thread::spawn(move || {
                Self::worker_impl(&task_queue, &cancel)
            })
        }).collect::<Vec<_>>();

        return Ok(ExecutorFailFastFuture { worker_handles: handles })
    }

    fn worker_impl<Err>(task_queue: &SchedulerInstance<Err>, cancel: &AtomicBool) -> Result<(), ExecutionError<Err>> {
        loop {
            if cancel.load(Ordering::Relaxed) {
                return Ok(())
            }

            let Some(task_id) = task_queue.get_next_wait()? else {
                return Ok(());
            };

            trace!("Starting task {}", task_id);

            task_queue.execute_task(&task_id)?;

            trace!("Completed task {task_id}");
        }
    }
}

#[derive(Debug)]
pub struct ExecutorFuture<Err> {
    worker_handles: Vec<JoinHandle<Result<(), SchedulerError>>>,
    errs: Arc<Mutex<Vec<TaskExecutionError<Err>>>>
}

impl<Err> ExecutorFuture<Err> {
    pub fn wait(self) -> Result<(), ExecutorFutureError<Err>> {
        for handle in self.worker_handles {
            handle.join()??;
        }

        let mut lock = self.errs.lock().map_err(|err| SchedulerError::from(err))?;
        let errs = lock.drain(..).collect::<Vec<_>>();
        if errs.len() > 0 {
            Err(ExecutorFutureError::TaskExecutionError(errs))
        } else {
            Ok(())
        }
    }

    pub fn is_finished(&self) -> bool {
        self.worker_handles.iter().find(|h| !h.is_finished()).is_none()
    }
}

#[derive(Debug)]
pub enum ExecutorFutureError<Err> {
    SchedulerError(SchedulerError),
    TaskExecutionError(Vec<TaskExecutionError<Err>>),
    JoinError(Box<dyn Any + Send>),
}

impl<Err> From<SchedulerError> for ExecutorFutureError<Err> {
    fn from(value: SchedulerError) -> Self {
        Self::SchedulerError(value)
    }
}

impl<Err> From<Box<dyn Any + Send>> for ExecutorFutureError<Err> {
    fn from(value: Box<dyn Any + Send>) -> Self {
        Self::JoinError(value)
    }
}

impl<Err: std::fmt::Debug> std::fmt::Display for ExecutorFutureError<Err> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}", self))
    }
}

impl<Err: std::fmt::Debug> std::error::Error for ExecutorFutureError<Err> {}

#[derive(Debug)]
pub struct ExecutorFailFastFuture<Err> {
    worker_handles: Vec<JoinHandle<Result<(), ExecutionError<Err>>>>,
}

impl<Err> ExecutorFailFastFuture<Err> {
    pub fn wait(self) -> Result<(), ExecutorFailFastFutureError<Err>> {
        for handle in self.worker_handles {
            handle.join()??;
        }

        Ok(())
    }

    pub fn is_finished(&self) -> bool {
        self.worker_handles.iter().find(|h| !h.is_finished()).is_none()
    }
}

#[derive(Debug)]
pub enum ExecutorFailFastFutureError<Err> {
    SchedulerError(SchedulerError),
    TaskExecutionError(TaskExecutionError<Err>),
    JoinError(Box<dyn Any + Send>),
}

impl<Err> From<ExecutionError<Err>> for ExecutorFailFastFutureError<Err> {
    fn from(value: ExecutionError<Err>) -> Self {
        match value {
            ExecutionError::SchedulerError(scheduler_error) => Self::SchedulerError(scheduler_error),
            ExecutionError::TaskExecutionError(task_execution_error) => Self::TaskExecutionError(task_execution_error),
        }
    }
}

impl<Err> From<Box<dyn Any + Send>> for ExecutorFailFastFutureError<Err> {
    fn from(value: Box<dyn Any + Send>) -> Self {
        Self::JoinError(value)
    }
}

impl<Err: std::fmt::Debug> std::fmt::Display for ExecutorFailFastFutureError<Err> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}", self))
    }
}

impl<Err: std::fmt::Debug> std::error::Error for ExecutorFailFastFutureError<Err> {}

#[derive(Debug)]
pub enum ExecutionError<Err> {
    SchedulerError(SchedulerError),
    TaskExecutionError(TaskExecutionError<Err>),
}

impl<Err> From<SchedulerError> for ExecutionError<Err> {
    fn from(value: SchedulerError) -> Self {
        Self::SchedulerError(value)
    }
}

impl<Err> From<TaskExecutionError<Err>> for ExecutionError<Err> {
    fn from(value: TaskExecutionError<Err>) -> Self {
        Self::TaskExecutionError(value)
    }
}

impl<Err: std::fmt::Debug> std::error::Error for ExecutionError<Err> {}

impl<Err: std::fmt::Debug> std::fmt::Display for ExecutionError<Err> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}", self))
    }
}
