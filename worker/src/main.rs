//! `cauli-worker`: the binary the source tree builds, and the name itest,
//! bench, PROTOCOL.md and both READMEs invoke.
//!
//! Deliberately three lines. The runtime lives in the crate's library target
//! (`src/lib.rs`) so that this entry point and `src/wheel_main.rs` can share
//! one compilation; two `[[bin]]` targets naming one source file is what
//! cargo reports as "file found to be present in multiple build targets".

fn main() -> ! {
    cauli_worker::run()
}
