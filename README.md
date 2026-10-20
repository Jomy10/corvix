<div align="center">
  <h1>Corvix</h1>
  <p>A task scheduler with dependency and priority support</p>
</div><br/>

## Example

```rust
use corvix::{Executor, Scheduler};

fn main() {
    let mut scheduler = Scheduler::new();
    let _1 = scheduler.add_task(|| {
        println!("1");
    }).unwrap();

    let _2 = scheduler.add_task(|| {
        println!("2");
    }).unwrap();

    let _3 = scheduler.add_task(|| {
        println!("3");
    }).unwrap();

    let _4 = scheduler.add_task(|| {
        std::thread::sleep(std::time::Duration::new(3, 0));
       println!("4");
    }).unwrap();

    scheduler.add_dependency(_2, _1);
    scheduler.add_dependency(_2, _3);
    scheduler.add_dependency(_3, _1);
    scheduler.add_dependency(_4, _3);

    let executor = Executor::new(None);
    executor.run(scheduler).unwrap()
        .wait().unwrap();
}
```

**Output**
```
1
3
2
4
```

More examples in the [examples folder](/examples).
