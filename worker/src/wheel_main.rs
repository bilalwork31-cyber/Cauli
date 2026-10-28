//! `cauli-worker-bin`: the same runtime under the name the wheel installs. The
//! wheel's `cauli-worker` console script (worker/wheel-data/scripts) points the
//! loader at libpython and execs this, so the two cannot share a file name.

fn main() -> ! {
    cauli_worker::run()
}
