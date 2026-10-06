//! Single-process benchmark: one publication and one subscription in one
//! process. `--mode=latency` measures the offer-to-poll round trip of one
//! message at a time; `--mode=throughput` offers batches of 1000 and drains.

use aeron_bench::{
    Counter, Pool, add_publication, add_subscription, aeron_dir, arg, arg_int, await_connected,
    connect, fail, is_fatal, mean_micros, offer_until_accepted, percentile_micros, realtime_ns,
    term_length,
};
use rusteron_client::{AeronPublication, AeronSubscription, Handler};
use std::time::{Duration, Instant};

const LABEL: &str = "bench-rust";

struct Run {
    size: usize,
    time: Duration,
    warmup: Duration,
}

fn main() {
    let mode = arg("mode", "latency");
    let run = Run {
        size: arg_int("size", 32) as usize,
        time: Duration::from_secs(arg_int("time", 10) as u64),
        warmup: Duration::from_secs(arg_int("warmup", 3) as u64),
    };
    eprintln!(
        "[{LABEL}] mode={mode} size={} time={:?} warmup={:?} dir={}",
        run.size,
        run.time,
        run.warmup,
        aeron_dir()
    );

    let client = connect(LABEL);
    let channel = format!(
        "aeron:ipc?alias=rust-bench-{}|term-length={}",
        realtime_ns(),
        term_length()
    );
    let stream = (realtime_ns() & 0x7FFF_FFFF) as i32;
    let publication = add_publication(&client, &channel, stream, LABEL);
    let subscription = add_subscription(&client, &channel, stream, LABEL);
    await_connected(&publication, &subscription, Duration::from_secs(10), LABEL);

    let mut pool = Pool::new(run.size);
    match mode.as_str() {
        "latency" => latency(&publication, &subscription, &mut pool, &run),
        "throughput" => throughput(&publication, &subscription, &mut pool, &run),
        other => fail(&format!("[{LABEL}] unknown mode {other}")),
    }
}

fn round_trip(
    publication: &AeronPublication,
    subscription: &AeronSubscription,
    handler: &Handler<Counter>,
    pool: &mut Pool,
    sequence: i64,
) {
    offer_until_accepted(publication, pool.message(sequence), LABEL);
    while subscription.poll(Some(handler), 1).unwrap_or(0) == 0 {}
}

fn latency(
    publication: &AeronPublication,
    subscription: &AeronSubscription,
    pool: &mut Pool,
    run: &Run,
) {
    let handler = Handler::new(Counter::counting());
    let mut sequence = 0;
    let warmup_deadline = Instant::now() + run.warmup;
    while Instant::now() < warmup_deadline {
        round_trip(publication, subscription, &handler, pool, sequence);
        sequence += 1;
    }

    let capacity = run.time.as_secs() as usize * 10_000_000;
    let mut samples: Vec<i64> = Vec::with_capacity(capacity);
    let start = Instant::now();
    let run_deadline = start + run.time;
    while samples.len() < capacity && Instant::now() < run_deadline {
        let started = Instant::now();
        round_trip(publication, subscription, &handler, pool, sequence);
        sequence += 1;
        samples.push(started.elapsed().as_nanos() as i64);
    }
    let elapsed = start.elapsed();

    let mean = mean_micros(&samples);
    samples.sort_unstable();
    println!(
        "{{\"client\":\"rust\",\"scenario\":\"latency\",\"payload_size\":{},\"samples\":{},\"ops_per_sec\":{:.3},\
         \"elapsed_ms\":{:.3},\"mean_us\":{:.4},\"median_us\":{:.4},\"p99_us\":{:.4},\"p999_us\":{:.4},\
         \"min_us\":{:.4},\"max_us\":{:.4}}}",
        run.size,
        samples.len(),
        samples.len() as f64 / elapsed.as_secs_f64(),
        elapsed.as_secs_f64() * 1000.0,
        mean,
        percentile_micros(&samples, 50.0),
        percentile_micros(&samples, 99.0),
        percentile_micros(&samples, 99.9),
        percentile_micros(&samples, 0.0),
        samples.last().copied().unwrap_or(0) as f64 / 1000.0
    );
}

fn throughput(
    publication: &AeronPublication,
    subscription: &AeronSubscription,
    pool: &mut Pool,
    run: &Run,
) {
    let handler = Handler::new(Counter::decoding());
    let poll = || subscription.poll(Some(&handler), 1024).unwrap_or(0);

    let mut sequence = 0;
    let warmup_deadline = Instant::now() + run.warmup;
    while Instant::now() < warmup_deadline {
        offer_until_accepted(publication, pool.message(sequence), LABEL);
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
    let run_deadline = start + run.time;
    while Instant::now() < run_deadline {
        for _ in 0..1000 {
            let message = pool.message(sequence);
            loop {
                match publication.offer(message) {
                    Ok(_) => break,
                    Err(error) if is_fatal(&error) => {
                        fail(&format!("[{LABEL}] publication closed: {error:?}"))
                    }
                    Err(_) => {
                        poll();
                    }
                }
            }
            sequence += 1;
            sent += 1;
        }
        poll();
    }
    let elapsed = start.elapsed();

    while handler.count < sent && poll() > 0 {}

    eprintln!("[{LABEL}] throughput tick_sum={}", handler.tick_sum);
    let ops_per_sec = sent as f64 / elapsed.as_secs_f64();
    println!(
        "{{\"client\":\"rust\",\"scenario\":\"throughput\",\"payload_size\":{},\"samples\":{},\"ops_per_sec\":{:.3},\
         \"bytes_per_sec\":{:.3},\"received\":{},\"elapsed_ms\":{:.3}}}",
        run.size,
        sent,
        ops_per_sec,
        ops_per_sec * run.size as f64,
        handler.count,
        elapsed.as_secs_f64() * 1000.0
    );
}
