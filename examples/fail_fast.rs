use std::thread::sleep;
use std::time::Duration;

use corvix::{AddTaskDescriptor, Executor, Scheduler};

#[derive(Debug)]
struct MyError {}

fn main() {
    let mut scheduler = Scheduler::new();
    let _1 = scheduler.add_task(|| {
        println!("1");
        Ok(())
    }, AddTaskDescriptor::default()).unwrap();

    let _2 = scheduler.add_task(|| {
        println!("2");
        Err(MyError{})
    }, AddTaskDescriptor::default()).unwrap();

    let _3 = scheduler.add_task(|| {
       println!("3");
       Ok(())
    }, AddTaskDescriptor::default()).unwrap();

    let _4 = scheduler.add_task(|| {
       println!("4");
       Ok(())
    }, AddTaskDescriptor::default()).unwrap();

    let _5 = scheduler.add_task(|| {
        sleep(Duration::from_secs(1));
       println!("5");
       Ok(())
    }, AddTaskDescriptor::default()).unwrap();

    scheduler.add_dependency(_2, _1);
    scheduler.add_dependency(_5, _1);
    scheduler.add_dependency(_3, _2);
    scheduler.add_dependency(_4, _2);

    let executor = Executor::new(None);
    if let Err(err) = executor.run_fail_fast(scheduler).unwrap().wait() {
        println!("{:#?}", err);
    }
}

// Output:
// 1
// 2
// TaskExecutionError(
//     Err(
//         f198ece2-08ec-4708-abce-b271bc032aa0,
//         MyError,
//     ),
// )
