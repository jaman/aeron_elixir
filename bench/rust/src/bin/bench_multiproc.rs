//! Multi-worker benchmark: `--workers=N` threads, each with its own client and
//! its own IPC publication and subscription, offering 1000 ticks at a time and
//! polling whenever the publication back-pressures.

use aeron_bench::{
    Counter, Pool, add_publication, add_subscription, aeron_dir, arg_int, await_connected, connect,
    fail, is_fatal, realtime_ns, term_length,
};
use rusteron_client::Handler;
use std::thread;
use std::time::{Duration, Instant};

const LABEL: &str = "bench-rust-mp";

struct Outcome {
    sent: i64,
    received: i64,
    elapsed: Duration,
}

fn main() {
    let workers = arg_int("workers", 4) as usize;
    let size = arg_int("size", 256) as usize;
    let time = Duration::from_secs(arg_int("time", 5) as u64);
    let warmup = Duration::from_secs(arg_int("warmup", 2) as u64);
    eprintln!(
        "[{LABEL}] workers={workers} size={size} time={time:?} warmup={warmup:?} dir={}",
        aeron_dir()
    );

    let outcomes: Vec<Outcome> = (0..workers)
        .map(|index| thread::spawn(move || run_worker(index, size, time, warmup)))
        .collect::<Vec<_>>()
        .into_iter()
        .map(|worker| {
            worker
                .join()
                .unwrap_or_else(|_| fail(&format!("[{LABEL}] worker panicked")))
        })
        .collect();

    let sent: i64 = outcomes.iter().map(|outcome| outcome.sent).sum();
    let received: i64 = outcomes.iter().map(|outcome| outcome.received).sum();
    let slowest = outcomes
        .iter()
        .map(|outcome| outcome.elapsed)
        .max()
        .unwrap_or_default();
    let ops_per_sec = sent as f64 / slowest.as_secs_f64();
    println!(
        "{{\"client\":\"rust\",\"scenario\":\"throughput\",\"payload_size\":{size},\"workers\":{workers},\
         \"samples\":{sent},\"received\":{received},\"ops_per_sec\":{ops_per_sec:.3},\"bytes_per_sec\":{:.3},\
         \"elapsed_ms\":{:.3}}}",
        ops_per_sec * size as f64,
        slowest.as_secs_f64() * 1000.0
    );
}

fn run_worker(index: usize, size: usize, time: Duration, warmup: Duration) -> Outcome {
    let client = connect(LABEL);
    let channel = format!(
        "aeron:ipc?alias=rust-mp-{index}-{}|term-length={}",
        realtime_ns(),
        term_length()
    );
    let stream = ((realtime_ns() + index as i64) & 0x7FFF_FFFF) as i32;
    let publication = add_publication(&client, &channel, stream, LABEL);
    let subscription = add_subscription(&client, &channel, stream, LABEL);
    await_connected(&publication, &subscription, Duration::from_secs(10), LABEL);

    let mut pool = Pool::new(size);
    let handler = Handler::new(Counter::decoding());
    let poll = || subscription.poll(Some(&handler), 1024).unwrap_or(0);
    let offer = |message: &[u8], drain: bool| loop {
        match publication.offer(message) {
            Ok(_) => return,
            Err(error) if is_fatal(&error) => fail(&format!(
                "[{LABEL}] worker {index} publication closed: {error:?}"
            )),
            Err(_) if drain => {
                poll();
            }
            Err(_) => {}
        }
    };

    let mut sequence = 0;
    let warmup_deadline = Instant::now() + warmup;
    while Instant::now() < warmup_deadline {
        offer(pool.message(sequence), false);
        sequence += 1;
        poll();
    }
    while poll() > 0 {}

    unsafe {
        let counter = handler.get_mut();
        counter.count = 0;
        counter.tick_sum = 0;
    }
    sequence = 0;
    let mut sent: i64 = 0;
    let start = Instant::now();
    let run_deadline = start + time;
    while Instant::now() < run_deadline {
        for _ in 0..1000 {
            offer(pool.message(sequence), true);
            sequence += 1;
            sent += 1;
        }
        poll();
    }
    let elapsed = start.elapsed();

    while handler.count < sent && poll() > 0 {}

    eprintln!("[{LABEL}] worker {index} tick_sum={}", handler.tick_sum);
    Outcome {
        sent,
        received: handler.count,
        elapsed,
    }
}
