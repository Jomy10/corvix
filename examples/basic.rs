use std::time::Duration;

use corvix::{AddTaskDescriptor, Executor, Scheduler};

fn main() {
    let mut scheduler = Scheduler::<()>::new();
    let _1 = scheduler.add_task(|| {
        println!("1");
        Ok(())
    }, AddTaskDescriptor::default()).unwrap();

    let _2 = scheduler.add_task(|| {
        println!("2");
        Ok(())
    }, AddTaskDescriptor::default()).unwrap();

    let _3 = scheduler.add_task(|| {
        println!("3");
        Ok(())
    }, AddTaskDescriptor::default()).unwrap();

    let _4 = scheduler.add_task(|| {
        std::thread::sleep(Duration::new(2, 0));
       println!("4");
       Ok(())
    }, AddTaskDescriptor::default()).unwrap();

    scheduler.add_dependency(_2, _1);
    scheduler.add_dependency(_2, _3);
    scheduler.add_dependency(_3, _1);
    scheduler.add_dependency(_4, _3);

    let executor = Executor::new(None);
    executor.run(scheduler).unwrap()
        .wait().unwrap();
}

// Output:
// 1
// 3
// 2
// 4
