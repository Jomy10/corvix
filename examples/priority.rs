use corvix::{AddTaskDescriptor, Executor, Scheduler};

fn main() {
    let mut scheduler = Scheduler::<()>::new();
    let _1 = scheduler.add_task(|| {
        println!("1");
        Ok(())
    }, AddTaskDescriptor {
        priority: 1,
        ..Default::default()
    }).unwrap();

    let _2 = scheduler.add_task(|| {
        println!("2");
        Ok(())
    }, AddTaskDescriptor::default()).unwrap();
    scheduler.change_priority(_2, 2);

    let _3 = scheduler.add_task(|| {
        println!("3");
        Ok(())
    }, AddTaskDescriptor {
        priority: 3,
        ..Default::default()
    }).unwrap();

    let _4 = scheduler.add_task(|| {
       println!("4");
       Ok(())
    }, AddTaskDescriptor {
        priority: 4,
        ..Default::default()
    }).unwrap();

    // Run with one thread
    let executor = Executor::new(Some(1.try_into().unwrap()));
    executor.run(scheduler).unwrap()
        .wait().unwrap();
}

// Output:
// 4
// 3
// 2
// 1
