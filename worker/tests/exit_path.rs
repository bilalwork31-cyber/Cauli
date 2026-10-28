//! Exit-path regression: a panic that unwinds out of the main thread must
//! reach `exit_now` (libc `_exit`), never ordinary libc `exit()`. See the
//! `exit_now` doc comment in src/main.rs for the incident this guards
//! against (OPENSSL_cleanup racing live threads during shutdown).
//!
//! No redis needed: the injected panic fires right after `PyRuntime::init`,
//! before `run_worker` ever touches a connection.
mod common;
use common::fixtures_dir;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Unique-enough temp path for this test run (pid + a monotonic counter),
/// avoiding a crates.io tempfile dependency for one marker file.
fn marker_path() -> PathBuf {
    use std::sync::atomic::{AtomicU32, Ordering};
    static SEQ: AtomicU32 = AtomicU32::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "cauli_exit_path_marker_{}_{n}.txt",
        std::process::id()
    ))
}

/// Spawn the real `cauli-worker` binary with the test-hooks env vars set:
/// `CAULI_TEST_ATEXIT_MARKER` registers a libc `atexit` handler that writes
/// `marker`, chosen because it directly observes the thing that matters --
/// whether the libc atexit/DSO-teardown path ran at all -- rather than
/// something probabilistic like heap corruption. `CAULI_TEST_PANIC_AFTER_PYRT_INIT`
/// forces a main-thread panic once the embedded interpreter's daemon asyncio
/// loop threads and the async-submit thread are already running, matching
/// the real incident's "threads still live at exit" condition.
fn spawn_and_wait(marker: &Path) -> i32 {
    let out = Command::new(env!("CARGO_BIN_EXE_cauli-worker"))
        .current_dir(fixtures_dir())
        .args(["--app", "fixture_app:app"])
        .env("CAULI_TEST_ATEXIT_MARKER", marker)
        .env("CAULI_TEST_PANIC_AFTER_PYRT_INIT", "1")
        .output()
        .expect("worker spawn");
    out.status.code().unwrap_or_else(|| {
        panic!(
            "worker did not exit via a normal exit code: {:?}",
            out.status
        )
    })
}

#[test]
fn main_thread_panic_after_pyrt_init_skips_atexit_handlers() {
    let marker = marker_path();
    let _ = std::fs::remove_file(&marker); // stale leftover from a killed run

    let code = spawn_and_wait(&marker);
    let atexit_ran = marker.exists();
    let _ = std::fs::remove_file(&marker);

    assert_eq!(code, 101, "main-thread panic must exit with code 101");
    assert!(
        !atexit_ran,
        "libc atexit handlers ran on the main-thread-panic exit path -- \
         exactly the live-threads-during-teardown race exit_now (src/main.rs) \
         exists to prevent"
    );
}

/// A fatal startup error is logged through the async log sink and the
/// process then `_exit`s: the line has to reach stdout before it does.
#[test]
fn a_fatal_startup_error_is_printed_before_exit() {
    let out = Command::new(env!("CARGO_BIN_EXE_cauli-worker"))
        .current_dir(fixtures_dir())
        .args(["--app", "fixture_app:app", "--batch", "0"])
        .output()
        .expect("worker spawn");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(1));
    assert!(
        stdout.contains("--batch must be >= 1"),
        "the startup error never reached stdout: {stdout:?}"
    );
}

/// A log reader that never reads must not stop the process exiting: the
/// sink's writer thread blocks holding the stdout lock once the pipe is
/// full, so an exit path that takes that lock never returns.
#[test]
fn a_stalled_log_reader_does_not_block_exit() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_cauli-worker"))
        .current_dir(fixtures_dir())
        // Every filler task trips the visibility timeout warning, far more
        // than a pipe buffer holds; the bad URL then exits 1 before redis.
        .args([
            "--app",
            "fixture_app:app",
            "--visibility-timeout",
            "1",
            "--redis-url",
            "not a url",
        ])
        .env("FIXTURE_FILLER_TASKS", "2000")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("worker spawn");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    let status = loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            break Some(status);
        }
        if std::time::Instant::now() >= deadline {
            break None;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    };
    let _ = child.kill();
    let _ = child.wait();
    let status =
        status.expect("worker still running 20s after a fatal error: exit is blocked on stdout");
    assert_eq!(status.code(), Some(1));
}

/// Nothing listens on this port: a refused connection, the startup case an
/// operator hits with redis down or a typo in the URL.
const UNREACHABLE_REDIS: &str = "redis://127.0.0.1:1/0";

/// With redis-rs's default backoff this run stayed silent for minutes.
/// Startup must give up within a bounded time and name what failed.
#[test]
fn an_unreachable_redis_fails_startup_quickly_and_says_why() {
    let started = std::time::Instant::now();
    let out = Command::new(env!("CARGO_BIN_EXE_cauli-worker"))
        .current_dir(fixtures_dir())
        .args(["--app", "fixture_app:app", "--redis-url", UNREACHABLE_REDIS])
        .output()
        .expect("worker spawn");
    let elapsed = started.elapsed();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(1), "stdout: {stdout:?}");
    assert!(
        stdout.contains("cannot connect to redis"),
        "the connection failure never reached stdout: {stdout:?}"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(30),
        "startup took {elapsed:?} to give up on an unreachable redis"
    );
}
