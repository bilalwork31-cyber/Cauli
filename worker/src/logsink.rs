//! A log sink that cannot block the caller.
//!
//! Writing stdout from tokio workers parks them all once a stalled pipe
//! reader fills the buffer, silently freezing fetch, ack and drain. So lines
//! go to a bounded channel drained by one blocking thread; when it is full a
//! line is dropped and the drop count is printed with the next line.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, Sender};

/// About 1.6 MB at 200 byte lines: rides out a stall of seconds, not minutes.
const CAPACITY: usize = 8192;

const FLUSH_POLL: Duration = Duration::from_millis(1);

/// Counters, so `flush` never takes the stdout lock a blocked writer holds.
#[derive(Default)]
struct Progress {
    queued: AtomicU64,
    written: AtomicU64,
}

/// Unset until `start` runs, for example on an argument parse error.
static PROGRESS: OnceLock<Arc<Progress>> = OnceLock::new();

#[derive(Clone)]
pub struct LogSink {
    tx: Sender<Vec<u8>>,
    dropped: Arc<AtomicU64>,
    progress: Arc<Progress>,
}

impl LogSink {
    pub fn start() -> Self {
        let (tx, rx) = bounded::<Vec<u8>>(CAPACITY);
        let dropped = Arc::new(AtomicU64::new(0));
        let seen = Arc::clone(&dropped);
        let progress = Arc::new(Progress::default());
        let done = Arc::clone(&progress);
        let _ = PROGRESS.set(Arc::clone(&progress));
        // A plain thread because it must be allowed to block. Never joined:
        // exit paths call the bounded `flush` instead.
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
                    done.written.fetch_add(1, Ordering::SeqCst);
                }
            })
            .expect("failed to spawn cauli-log thread");
        LogSink {
            tx,
            dropped,
            progress,
        }
    }
}

/// Wait at most `budget` for every line queued before this call to reach
/// stdout. Bounded because a stalled reader must not stop the process exiting.
pub fn flush(budget: Duration) {
    let Some(p) = PROGRESS.get() else { return };
    let target = p.queued.load(Ordering::SeqCst);
    let deadline = Instant::now() + budget;
    while p.written.load(Ordering::SeqCst) < target && Instant::now() < deadline {
        std::thread::sleep(FLUSH_POLL);
    }
}

/// One event's bytes. The fmt layer drops the writer once the line is
/// complete, so `Drop` hands over whole lines only.
pub struct LineBuf {
    buf: Vec<u8>,
    tx: Sender<Vec<u8>>,
    dropped: Arc<AtomicU64>,
    progress: Arc<Progress>,
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
        // Counted before the send, so a concurrent `flush` waits for it too.
        self.progress.queued.fetch_add(1, Ordering::SeqCst);
        // try_send: a blocking send would bring back the stall.
        if self.tx.try_send(line).is_err() {
            self.progress.queued.fetch_sub(1, Ordering::SeqCst);
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
            progress: Arc::clone(&self.progress),
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
        // No writer thread, so the channel stays full.
        let (tx, _rx) = bounded::<Vec<u8>>(2);
        let dropped = Arc::new(AtomicU64::new(0));
        let sink = LogSink {
            tx,
            dropped: Arc::clone(&dropped),
            progress: Default::default(),
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
        let sink = LogSink {
            tx,
            dropped,
            progress: Default::default(),
        };
        drop(sink.make_writer());
        assert!(rx.try_recv().is_err());
    }
}
