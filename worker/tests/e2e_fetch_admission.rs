//! A worker never takes more entries off the stream than it can start: the
//! surplus would sit in its pending list with the recovery clock running
//! while another worker, or another process of the same worker, stays idle.
mod common;
use common::*;
use serde_json::json;
use std::time::Duration;

const SLOTS: i64 = 4;
const QUEUED: i64 = 20;

#[tokio::test(flavor = "multi_thread")]
async fn a_fetch_takes_no_more_entries_than_free_io_slots() {
    start_redis();
    let mut c = conn().await;
    let _: String = redis::cmd("FLUSHALL").query_async(&mut c).await.unwrap();
    let slots = SLOTS.to_string();
    let _w = Worker::spawn("fadm", &["--io-concurrency", &slots]);
    wait_group(&mut c, "fadm", 20).await;

    for _ in 0..QUEUED {
        let (_id, e) = envelope("fx.aslow", "fadm", |v| v["args"] = json!([30.0]));
        xadd(&mut c, "fadm", &e.to_string()).await;
    }

    // Long enough for several fetch rounds; every slot is taken by a task
    // that sleeps far past the end of the test.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let delivered = xpending_count(&mut c, "fadm").await;
    assert_eq!(
        delivered, SLOTS,
        "{delivered} of {QUEUED} entries delivered to a worker with {SLOTS} io slots"
    );
}
