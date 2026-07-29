use std::thread::sleep;
use std::time::Duration;

use corvix::{AddTaskDescriptor, Executor, Scheduler};

#[derive(Debug)]
struct MyError {}

fn main() {
    let mut scheduler = Scheduler::<MyError>::new();
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
    if let Err(err) = executor.run(scheduler).unwrap().wait() {
        println!("{:#?}", err);
    }
}

// Output:
// 2
// 5
// TaskExecutionError(
//     [
//         Err(
//             a02a02f6-91b0-406a-9412-29ecbf22bee7,
//             MyError,
//         ),
//         Cancelled(
//             e0dcf3b5-4899-48a9-8d81-c1dbe4a96dbe,
//         ),
//         Cancelled(
//             6e718783-e907-4fa6-b959-352b135a05d7,
//         ),
//     ],
// )
