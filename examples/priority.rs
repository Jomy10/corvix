use corvix::{Executor, Scheduler};

fn main() {
    let mut scheduler = Scheduler::new();
    let _1 = scheduler.add_task_with_priority(|| {
        println!("1");
    }, 1).unwrap();

    let _2 = scheduler.add_task(|| {
        println!("2");
    }).unwrap();
    scheduler.change_priority(_2, 2);

    let _3 = scheduler.add_task_with_priority(|| {
        println!("3");
    }, 3).unwrap();

    let _4 = scheduler.add_task_with_priority(|| {
       println!("4");
    }, 4).unwrap();

    let executor = Executor::new(Some(1.try_into().unwrap()));
    executor.run(scheduler).unwrap()
        .wait().unwrap();
}

// Output:
// 4
// 3
// 2
// 1
