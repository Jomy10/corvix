use std::any::Any;
use std::num::NonZero;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use log::*;

use crate::{Scheduler, SchedulerError, SchedulerInstance, TaskExecutionError};

pub struct Executor {
    num_workers: NonZero<usize>,
    on_task_finish: Option<Arc<dyn Fn() + Send + Sync + 'static>>,
}

impl Executor {
    /// An executor executes the tasks defined in a `Scheduler`, based on the
    /// dependencies and priorities.
    ///
    /// # Params
    /// - `num_workers`: When passed, sets the amount of worker threads to spawn.
    ///   Defaults to the available CPU's (`std::thread::available_parallelism`).
    pub fn new(num_workers: Option<NonZero<usize>>) -> Self {
        let nproc = num_workers
            .unwrap_or(std::thread::available_parallelism().unwrap_or(NonZero::new(3usize).unwrap()));

        Executor {
            num_workers: nproc,
            on_task_finish: None
        }
    }

    /// Execute an arbitrary function when a task is finished. Can for example be
    /// used with `indicatif` for a incrementing a progressbar.
    pub fn set_on_task_finish(&mut self, on_task_finish: impl Fn() + Send + Sync + 'static) {
        self.on_task_finish = Some(Arc::new(on_task_finish))
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
            let on_task_finish = self.on_task_finish.as_ref().map(|f| f.clone());
            thread::spawn(move || {
                let res = std::panic::catch_unwind(AssertUnwindSafe(|| {
                    loop {
                        match Self::worker_impl(&task_queue, &cancel, on_task_finish.as_ref()) {
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
                }));

                match res {
                    Ok(res) => res,
                    Err(err) => {
                        let mut errs = errs.lock()?;
                        errs.push(TaskExecutionError::Panic(err));
                        Ok(())
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
            let on_task_finish = self.on_task_finish.as_ref().map(|f| f.clone());
            thread::spawn(move || {
                let res = std::panic::catch_unwind(AssertUnwindSafe(|| {
                    Self::worker_impl(&task_queue, &cancel, on_task_finish.as_ref())
                }));

                match res {
                    Ok(res) => res,
                    Err(err) => {
                        Err(ExecutionError::TaskExecutionError(TaskExecutionError::Panic(err)))
                    },
                }
            })
        }).collect::<Vec<_>>();

        return Ok(ExecutorFailFastFuture { worker_handles: handles })
    }

    fn worker_impl<Err>(task_queue: &SchedulerInstance<Err>, cancel: &AtomicBool, on_finish: Option<&Arc<dyn Fn() + Send + Sync + 'static>>) -> Result<(), ExecutionError<Err>> {
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

            if let Some(on_finish) = &on_finish {
                on_finish()
            }
        }
    }
}

#[derive(Debug)]
pub struct ExecutorFuture<Err> {
    worker_handles: Vec<JoinHandle<Result<(), SchedulerError>>>,
    errs: Arc<Mutex<Vec<TaskExecutionError<Err>>>>
}

impl<Err> ExecutorFuture<Err> {
    /// Block the current thread until execution has stopped
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

    /// Check if all workers have finished
    pub fn is_finished(&self) -> bool {
        self.worker_handles.iter().find(|h| !h.is_finished()).is_none()
    }
}

#[derive(Debug)]
pub enum ExecutorFutureError<Err> {
    /// Error during scheduling of tasks
    SchedulerError(SchedulerError),
    /// Error happened during task execution
    TaskExecutionError(Vec<TaskExecutionError<Err>>),
    /// Error happened in `wait` while joining the worker threads
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
