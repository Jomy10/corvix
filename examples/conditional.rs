use std::time::Duration;

use corvix::{AddTaskDescriptor, Conditional, Executor, Scheduler};

fn main() {
    let mut scheduler = Scheduler::<()>::new();

    let mut cond = Conditional::new();
    scheduler.add_task(|| {
        println!("Task ran");
        Ok(())
    }, AddTaskDescriptor {
        condition: Some(&mut cond),
        ..Default::default()
    }).unwrap();

    let executor = Executor::new(None);
    let future = executor.run(scheduler).unwrap();

    std::thread::sleep(Duration::from_secs(2));
    println!("Resolving conditional...");
    cond.resolve();

    future.wait().unwrap();
}

// Output after 2 seconds:
// Resolving conditional...
// Task ran
