//! `cauli-worker-bin`: the identical runtime under the name the published
//! wheel installs.
//!
//! The wheel puts a Python console script at `cauli-worker` in the venv's bin
//! directory (worker/wheel-data/scripts/cauli-worker); it points the dynamic
//! loader at the running interpreter's libpython and then execs this binary,
//! so the two cannot share one file name. Same program as `src/main.rs`, same
//! `cauli_worker::run`; only the produced file name differs, and the
//! `dev-bin` / `wheel-bin` features decide which one a build produces.

fn main() -> ! {
    cauli_worker::run()
}
