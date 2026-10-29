//! The app the worker imports sees the Redis the worker consumes from: a task
//! that enqueues a follow up task must not send it to another Redis. The
//! supervisor already exports it to its child processes; one process must too.
mod common;
use common::*;

#[tokio::test(flavor = "multi_thread")]
async fn the_app_environment_carries_the_workers_redis_url() {
    start_redis();
    let mut c = conn().await;
    let _: String = redis::cmd("FLUSHALL").query_async(&mut c).await.unwrap();
    let _w = Worker::spawn("appenv", &[]);
    wait_group(&mut c, "appenv", 20).await;

    let (id, e) = envelope("fx.redis_env", "appenv", |_| {});
    xadd(&mut c, "appenv", &e.to_string()).await;
    let result = wait_result(&mut c, &id, 20).await;
    assert_eq!(
        result["result"].as_str(),
        Some(redis_url().as_str()),
        "the app saw {:?} instead of the worker's --redis-url",
        result["result"]
    );
}
