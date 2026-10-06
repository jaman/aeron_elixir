//! Request/response benchmark. `--mode=ping` sends one 32-byte request at a
//! time and waits for its echo; `--mode=pong` echoes every request until
//! SIGTERM. Streams come from `RPC_STREAM_BASE` and `RPC_PAIR_INDEX`.
//! After its warmup the pinger waits until the wall clock reaches
//! `RPC_START_AT_NS`, then times its loop and reports the loop's wall-clock
//! start and finish.

use aeron_bench::{
    Counter, Pool, add_publication, add_subscription, aeron_dir, arg, await_connected, connect,
    env_int, fail, is_fatal, mean_micros, offer_until_accepted, percentile_micros, read_tick,
    realtime_ns,
};
use rusteron_client::{AeronFragmentHandlerCallback, AeronHeader, AeronPublication, Handler};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const LABEL: &str = "bench-rust-rpc";
const START_SPIN_NS: i64 = 2_000_000;

fn await_start(start_at_ns: i64) {
    let sleep_ns = start_at_ns - realtime_ns() - START_SPIN_NS;
    if sleep_ns > 0 {
        std::thread::sleep(Duration::from_nanos(sleep_ns as u64));
    }
    while realtime_ns() < start_at_ns {}
}

struct Streams {
    ping: i32,
    pong: i32,
}

fn main() {
    let mode = arg("mode", "ping");
    let base = env_int("RPC_STREAM_BASE", 7100);
    let pair_index = env_int("RPC_PAIR_INDEX", 0);
    let streams = Streams {
        ping: (base + pair_index * 2) as i32,
        pong: (base + pair_index * 2 + 1) as i32,
    };
    let length = env_int("RPC_MESSAGE_LENGTH", 32) as usize;
    eprintln!(
        "[{LABEL}] mode={mode} ping={} pong={} length={length} dir={}",
        streams.ping,
        streams.pong,
        aeron_dir()
    );

    match mode.as_str() {
        "ping" => ping(
            &streams,
            length,
            env_int("RPC_WARMUP_MESSAGES", 100_000),
            env_int("RPC_MESSAGES", 1_000_000),
            env_int("RPC_START_AT_NS", 0),
        ),
        "pong" => pong(&streams),
        other => fail(&format!("[{LABEL}] unknown mode {other}")),
    }
}

fn ping(streams: &Streams, length: usize, warmup: i64, messages: i64, start_at_ns: i64) {
    let client = connect(LABEL);
    let subscription = add_subscription(&client, "aeron:ipc", streams.pong, LABEL);
    let publication = add_publication(&client, "aeron:ipc", streams.ping, LABEL);
    await_connected(&publication, &subscription, Duration::from_secs(30), LABEL);

    let mut pool = Pool::new(length);
    let handler = Handler::new(Counter::decoding());
    let mut sequence = 0;
    let mut round_trip = || {
        offer_until_accepted(&publication, pool.message(sequence), LABEL);
        sequence += 1;
        while subscription.poll(Some(&handler), 1).unwrap_or(0) == 0 {}
    };

    for _ in 0..warmup {
        round_trip();
    }

    let mut samples = vec![0i64; messages as usize];
    await_start(start_at_ns);
    let started_at_ns = realtime_ns();
    let start = Instant::now();
    for sample in samples.iter_mut() {
        let started = Instant::now();
        round_trip();
        *sample = started.elapsed().as_nanos() as i64;
    }
    let elapsed = start.elapsed();
    let finished_at_ns = realtime_ns();

    let mean = mean_micros(&samples);
    samples.sort_unstable();
    eprintln!("[{LABEL}] tick_sum={}", handler.tick_sum);
    println!(
        "{{\"client\":\"rust\",\"scenario\":\"rpc\",\"pairs\":1,\"message_length\":{length},\"samples\":{messages},\
         \"elapsed_ms\":{:.3},\"mean_us\":{mean:.4},\"p50_us\":{:.4},\"p90_us\":{:.4},\"p99_us\":{:.4},\
         \"p999_us\":{:.4},\"max_us\":{:.4},\"round_trips_per_sec\":{:.3},\"started_at_ns\":{started_at_ns},\
         \"finished_at_ns\":{finished_at_ns}}}",
        elapsed.as_secs_f64() * 1000.0,
        percentile_micros(&samples, 50.0),
        percentile_micros(&samples, 90.0),
        percentile_micros(&samples, 99.0),
        percentile_micros(&samples, 99.9),
        samples.last().copied().unwrap_or(0) as f64 / 1000.0,
        messages as f64 / elapsed.as_secs_f64()
    );
}

struct Echo {
    publication: AeronPublication,
    running: Arc<AtomicBool>,
    received: i64,
    tick_sum: i64,
}

impl AeronFragmentHandlerCallback for Echo {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
        self.received += 1;
        self.tick_sum += read_tick(buffer);
        while self.running.load(Ordering::Acquire) {
            match self.publication.offer(buffer) {
                Ok(_) => return,
                Err(error) if is_fatal(&error) => {
                    eprintln!("[{LABEL}] echo failed: {error:?}");
                    self.running.store(false, Ordering::Release);
                    return;
                }
                Err(_) => {}
            }
        }
    }
}

fn pong(streams: &Streams) {
    let running = Arc::new(AtomicBool::new(true));
    let stop = Arc::clone(&running);
    ctrlc::set_handler(move || stop.store(false, Ordering::Release))
        .unwrap_or_else(|error| fail(&format!("[{LABEL}] signal handler: {error}")));

    let client = connect(LABEL);
    let subscription = add_subscription(&client, "aeron:ipc", streams.ping, LABEL);
    let publication = add_publication(&client, "aeron:ipc", streams.pong, LABEL);
    println!("READY");
    await_connected(&publication, &subscription, Duration::from_secs(30), LABEL);

    let handler = Handler::new(Echo {
        publication,
        running: Arc::clone(&running),
        received: 0,
        tick_sum: 0,
    });
    while running.load(Ordering::Acquire) {
        let _ = subscription.poll(Some(&handler), 1);
    }
    eprintln!(
        "[{LABEL}] pong tick_sum={} received={}",
        handler.tick_sum, handler.received
    );
}
