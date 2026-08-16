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
