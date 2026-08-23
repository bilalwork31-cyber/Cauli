//! A log sink that cannot block the caller.
//!
//! `tracing_subscriber::fmt()` writes straight to stdout from whichever thread
//! emitted the event. Every tokio runtime worker reaches per-envelope
//! `warn!`/`error!` sites (dispatch.rs, exec.rs), so when stdout is a pipe
//! whose reader stalls -- a paused `docker logs`, a wedged sidecar, a full
//! journal disk -- the 64 KiB pipe buffer fills, `write(2)` blocks, and the
//! runtime workers park inside it one by one. The worker then stops fetching,
//! stops acking and stops draining, with no error, no counter and no wedge
//! detection: it is the only genuine blocking-in-async path in the crate.
//!
//! So the emitting thread hands its formatted line to a bounded channel and
//! returns. One dedicated thread does the blocking write. When the channel is
//! full the line is DROPPED and counted, and the count is reported inline the
//! next time a write succeeds. Losing log lines during a log-transport outage
//! is the right trade against freezing task execution during one, and the gap
//! is never silent.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crossbeam_channel::{bounded, Sender};

/// Formatted lines held in memory before the sink starts dropping them. At a
/// typical ~200 byte structured line this bounds the sink near 1.6 MB, which
/// is small against the worker's own footprint and deep enough to ride out a
/// reader that stalls for seconds rather than minutes.
const CAPACITY: usize = 8192;

#[derive(Clone)]
pub struct LogSink {
    tx: Sender<Vec<u8>>,
    dropped: Arc<AtomicU64>,
}

impl LogSink {
    /// Spawns the writer thread and returns the sink to hand to
    /// `tracing_subscriber::fmt().with_writer(..)`.
    pub fn start() -> Self {
        let (tx, rx) = bounded::<Vec<u8>>(CAPACITY);
        let dropped = Arc::new(AtomicU64::new(0));
        let seen = Arc::clone(&dropped);
        // Not a tokio task: this thread is ALLOWED to block, which is the
        // whole point. It exits when the sender is dropped, which happens only
        // at process teardown (the subscriber owns the sink for the process
        // lifetime), so lines still queued when the process exits are lost --
        // the same lines a blocking writer would have been stuck inside.
        std::thread::Builder::new()
            .name("cauli-log".into())
            .spawn(move || {
                let mut reported = 0u64;
                let out = io::stdout();
                while let Ok(line) = rx.recv() {
                    let mut h = out.lock();
                    let _ = io::Write::write_all(&mut h, &line);
                    let now = seen.load(Ordering::Relaxed);
                    if now > reported {
                        let _ = io::Write::write_all(
                            &mut h,
                            format!(
                                "cauli: log sink dropped {} line(s); \
                                 the log reader could not keep up\n",
                                now - reported
                            )
                            .as_bytes(),
                        );
                        reported = now;
                    }
                    let _ = io::Write::flush(&mut h);
                }
            })
            .expect("failed to spawn cauli-log thread");
        LogSink { tx, dropped }
    }
}

/// One event's formatted bytes. `tracing_subscriber`'s fmt layer takes a fresh
/// writer per event and drops it when the line is complete, so `Drop` is where
/// a whole line is handed over -- never a partial one.
pub struct LineBuf {
    buf: Vec<u8>,
    tx: Sender<Vec<u8>>,
    dropped: Arc<AtomicU64>,
}

impl io::Write for LineBuf {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for LineBuf {
    fn drop(&mut self) {
        if self.buf.is_empty() {
            return;
        }
        let line = std::mem::take(&mut self.buf);
        // try_send, never send: blocking here would reintroduce exactly the
        // stall this module exists to remove, just one queue further out.
        if self.tx.try_send(line).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogSink {
    type Writer = LineBuf;

    fn make_writer(&'a self) -> LineBuf {
        LineBuf {
            buf: Vec::with_capacity(256),
            tx: self.tx.clone(),
            dropped: Arc::clone(&self.dropped),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use tracing_subscriber::fmt::MakeWriter as _;

    #[test]
    fn a_full_sink_drops_instead_of_blocking() {
        // A sink whose writer thread never runs: the channel fills and stays
        // full, so every later line must be counted and discarded rather than
        // parking the caller.
        let (tx, _rx) = bounded::<Vec<u8>>(2);
        let dropped = Arc::new(AtomicU64::new(0));
        let sink = LogSink {
            tx,
            dropped: Arc::clone(&dropped),
        };
        for _ in 0..10 {
            let mut w = sink.make_writer();
            w.write_all(b"a line\n").unwrap();
            drop(w);
        }
        assert_eq!(dropped.load(Ordering::Relaxed), 8, "2 queued, 8 dropped");
    }

    #[test]
    fn an_empty_line_is_never_sent() {
        let (tx, rx) = bounded::<Vec<u8>>(4);
        let dropped = Arc::new(AtomicU64::new(0));
        let sink = LogSink { tx, dropped };
        drop(sink.make_writer());
        assert!(rx.try_recv().is_err());
    }
}
