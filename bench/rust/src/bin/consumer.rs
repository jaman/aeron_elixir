//! Market-data consumer for the harness: subscribes to the price stream, decodes
//! every tick, records publish-to-read latency in a 100 ns histogram and prints a
//! JSON report once the stream has been idle for 750 ms or the duration ends.

use aeron_bench::{add_subscription, connect, env_int, env_or, fail, realtime_ns};
use rusteron_client::{AeronFragmentHandlerCallback, AeronHeader, Handler};
use std::time::{Duration, Instant};

const BUCKETS: usize = 100_000;
const BUCKET_NS: i64 = 100;
const IDLE_EXIT: Duration = Duration::from_millis(750);
const TICK_FIELDS_LENGTH: usize = 36;

struct Consumer {
    consumed: i64,
    first_sequence: i64,
    last_sequence: i64,
    gaps: i64,
    price_sum: i64,
    negatives: i64,
    max_ns: i64,
    counts: Vec<i64>,
}

impl AeronFragmentHandlerCallback for Consumer {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
        if buffer.len() < TICK_FIELDS_LENGTH {
            return;
        }
        let instrument_id = u32::from_le_bytes(buffer[0..4].try_into().unwrap()) as i64;
        let bid = i64::from_le_bytes(buffer[4..12].try_into().unwrap());
        let ask = i64::from_le_bytes(buffer[12..20].try_into().unwrap());
        let sequence = i64::from_le_bytes(buffer[20..28].try_into().unwrap());
        let published_at = i64::from_le_bytes(buffer[28..36].try_into().unwrap());

        let latency = realtime_ns() - published_at;
        if latency < 0 {
            self.negatives += 1;
        } else {
            self.counts[((latency / BUCKET_NS) as usize).min(BUCKETS - 1)] += 1;
            self.max_ns = self.max_ns.max(latency);
        }

        if self.first_sequence < 0 {
            self.first_sequence = sequence;
        } else if sequence != self.last_sequence + 1 {
            self.gaps += 1;
        }
        self.last_sequence = sequence;
        self.consumed += 1;
        self.price_sum += bid + ask + instrument_id;
    }
}

impl Consumer {
    fn percentile(&self, total: i64, fraction: f64) -> f64 {
        if total == 0 {
            return 0.0;
        }
        let target = ((total as f64 * fraction) as i64).max(1);
        let mut seen = 0;
        for (index, count) in self.counts.iter().enumerate() {
            seen += count;
            if seen >= target {
                return (index as f64 * BUCKET_NS as f64 + BUCKET_NS as f64 / 2.0) / 1000.0;
            }
        }
        0.0
    }
}

fn main() {
    let channel = env_or(
        "HARNESS_CHANNEL",
        "aeron:ipc?alias=prices|term-length=67108864",
    );
    let stream = env_int("HARNESS_STREAM", 9001) as i32;
    let duration = Duration::from_secs((env_int("HARNESS_DURATION_S", 30) + 10) as u64);
    let label = env_or("HARNESS_LABEL", "rust");
    let tag = format!("consumer-{label}");

    let client = connect(&tag);
    let subscription = add_subscription(&client, &channel, stream, &tag);
    println!("READY");
    eprintln!("[{tag}] subscribed, awaiting image");
    while !subscription.is_connected() {
        std::thread::yield_now();
    }
    eprintln!("[{tag}] image attached");

    let consumer = Consumer {
        consumed: 0,
        first_sequence: -1,
        last_sequence: -1,
        gaps: 0,
        price_sum: 0,
        negatives: 0,
        max_ns: 0,
        counts: vec![0; BUCKETS],
    };
    let (assembler, state) = Handler::with_fragment_assembler(consumer)
        .unwrap_or_else(|error| fail(&format!("[{tag}] fragment assembler: {error:?}")));

    let deadline = Instant::now() + duration;
    let mut last_message_at = Instant::now();
    loop {
        let now = Instant::now();
        if now >= deadline
            || (state.consumed > 0 && now.duration_since(last_message_at) > IDLE_EXIT)
        {
            break;
        }
        if subscription.poll(Some(&assembler), 1024).unwrap_or(0) > 0 {
            last_message_at = now;
        }
    }

    let total: i64 = state.counts.iter().sum();
    let weighted: f64 = state
        .counts
        .iter()
        .enumerate()
        .map(|(index, &count)| {
            count as f64 * (index as f64 * BUCKET_NS as f64 + BUCKET_NS as f64 / 2.0)
        })
        .sum();
    let mean_us = if total > 0 {
        weighted / total as f64 / 1000.0
    } else {
        0.0
    };

    eprintln!("[{tag}] price_sum={}", state.price_sum);
    println!(
        "{{\"role\":\"consumer\",\"client\":\"{label}\",\"consumed\":{},\"first_sequence\":{},\"last_sequence\":{},\
         \"sequence_span\":{},\"gaps\":{},\"p50_us\":{:.3},\"p90_us\":{:.3},\"p99_us\":{:.3},\"p999_us\":{:.3},\
         \"max_us\":{:.3},\"mean_us\":{mean_us:.3},\"negative_latencies\":{}}}",
        state.consumed,
        state.first_sequence,
        state.last_sequence,
        state.last_sequence - state.first_sequence + 1,
        state.gaps,
        state.percentile(total, 0.5),
        state.percentile(total, 0.9),
        state.percentile(total, 0.99),
        state.percentile(total, 0.999),
        state.max_ns as f64 / 1000.0,
        state.negatives
    );
}
