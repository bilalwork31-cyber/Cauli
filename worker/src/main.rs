//! `cauli-worker`: the binary name the source tree builds. The runtime lives
//! in `src/lib.rs` so this and `src/wheel_main.rs` share one compilation.

fn main() -> ! {
    cauli_worker::run()
}
