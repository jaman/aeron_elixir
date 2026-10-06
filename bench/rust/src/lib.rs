//! Shared pieces of the Rust (rusteron) benchmark programs: argument and
//! environment reading, connecting a client, the payload pool every client in
//! the benchmarks builds, tick encoding and decoding, and sample statistics.

use rusteron_client::{
    Aeron, AeronContext, AeronFragmentHandlerCallback, AeronHeader, AeronOfferError,
    AeronPublication, AeronSubscription, Handlers,
};
use std::ffi::CString;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Length of the tick stamped over the start of every payload.
pub const TICK_LENGTH: usize = 28;

/// Number of entries in the payload pool.
pub const POOL_ENTRIES: usize = 4096;

const POOL_SEED: u64 = 0x5EED_AE20;

/// Returns the environment variable `name`, or `fallback` when it is unset or empty.
pub fn env_or(name: &str, fallback: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

/// Returns the environment variable `name` as an integer, or `fallback`.
pub fn env_int(name: &str, fallback: i64) -> i64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

/// Returns the value of the `--name=value` command-line argument, or `fallback`.
pub fn arg(name: &str, fallback: &str) -> String {
    let prefix = format!("--{name}=");
    std::env::args()
        .find_map(|argument| argument.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| fallback.to_string())
}

/// Returns the `--name=value` argument as an integer, or `fallback`.
pub fn arg_int(name: &str, fallback: i64) -> i64 {
    arg(name, &fallback.to_string()).parse().unwrap_or(fallback)
}

/// The media driver directory: `AERON_DIR`, default `/tmp/ae_drv`.
pub fn aeron_dir() -> String {
    env_or("AERON_DIR", "/tmp/ae_drv")
}

/// The term length for per-run channels: `BENCH_TERM_LENGTH`, default 16 MiB.
pub fn term_length() -> String {
    env_or("BENCH_TERM_LENGTH", "16777216")
}

/// Prints `message` to stderr and exits with status 1.
pub fn fail(message: &str) -> ! {
    eprintln!("{message}");
    std::process::exit(1)
}

/// A started rusteron client and the context it was built from.
pub struct Client {
    pub aeron: Aeron,
    _context: AeronContext,
}

/// Connects a client to the driver at [`aeron_dir`], or exits with an error.
pub fn connect(label: &str) -> Client {
    let context =
        AeronContext::new().unwrap_or_else(|error| fail(&format!("[{label}] context: {error:?}")));
    let dir =
        CString::new(aeron_dir()).unwrap_or_else(|_| fail(&format!("[{label}] invalid AERON_DIR")));
    context
        .set_dir(&dir)
        .unwrap_or_else(|error| fail(&format!("[{label}] set dir: {error:?}")));
    let aeron =
        Aeron::new(&context).unwrap_or_else(|error| fail(&format!("[{label}] connect: {error:?}")));
    aeron
        .start()
        .unwrap_or_else(|error| fail(&format!("[{label}] start: {error:?}")));
    Client {
        aeron,
        _context: context,
    }
}

/// Adds a publication on `channel` and `stream`, waiting up to 10 s, or exits.
pub fn add_publication(
    client: &Client,
    channel: &str,
    stream: i32,
    label: &str,
) -> AeronPublication {
    let channel =
        CString::new(channel).unwrap_or_else(|_| fail(&format!("[{label}] invalid channel")));
    client
        .aeron
        .async_add_publication(&channel, stream)
        .and_then(|pending| pending.poll_blocking(Duration::from_secs(10)))
        .unwrap_or_else(|error| fail(&format!("[{label}] add publication: {error:?}")))
}

/// Adds a subscription on `channel` and `stream`, waiting up to 10 s, or exits.
pub fn add_subscription(
    client: &Client,
    channel: &str,
    stream: i32,
    label: &str,
) -> AeronSubscription {
    let channel =
        CString::new(channel).unwrap_or_else(|_| fail(&format!("[{label}] invalid channel")));
    client
        .aeron
        .async_add_subscription(&channel, stream, Handlers::NONE, Handlers::NONE)
        .and_then(|pending| pending.poll_blocking(Duration::from_secs(10)))
        .unwrap_or_else(|error| fail(&format!("[{label}] add subscription: {error:?}")))
}

/// Waits until `publication` has a subscriber and `subscription` has an image,
/// failing after `timeout`.
pub fn await_connected(
    publication: &AeronPublication,
    subscription: &AeronSubscription,
    timeout: Duration,
    label: &str,
) {
    let deadline = Instant::now() + timeout;
    while !(publication.is_connected() && subscription.is_connected()) {
        if Instant::now() > deadline {
            fail(&format!(
                "[{label}] publication and subscription not connected within {timeout:?}"
            ));
        }
        std::thread::yield_now();
    }
}

/// Whether an offer result ends the publication: closed or out of positions.
pub fn is_fatal(error: &AeronOfferError) -> bool {
    matches!(
        error,
        AeronOfferError::Closed | AeronOfferError::MaxPositionExceeded
    )
}

/// Offers `payload` until it is accepted, exiting when the publication is closed.
pub fn offer_until_accepted(publication: &AeronPublication, payload: &[u8], label: &str) {
    loop {
        match publication.offer(payload) {
            Ok(_) => return,
            Err(error) if is_fatal(&error) => {
                fail(&format!("[{label}] publication closed: {error:?}"))
            }
            Err(_) => {}
        }
    }
}

/// The 4096 payloads every benchmark client builds: one splitmix64 byte stream
/// (seed `0x5EEDAE20`, little-endian words) cut into `size`-byte entries.
pub struct Pool {
    bytes: Vec<u8>,
    size: usize,
}

impl Pool {
    /// Builds the pool of `size`-byte payloads.
    pub fn new(size: usize) -> Self {
        let total = POOL_ENTRIES * size;
        let mut bytes = Vec::with_capacity(total.div_ceil(8) * 8);
        let mut state = POOL_SEED;
        while bytes.len() < total {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut mixed = state;
            mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            mixed ^= mixed >> 31;
            bytes.extend_from_slice(&mixed.to_le_bytes());
        }
        bytes.truncate(total);
        Pool { bytes, size }
    }

    /// Stamps the tick for `sequence` into pool entry `sequence mod 4096` and returns it.
    pub fn message(&mut self, sequence: i64) -> &[u8] {
        let start = (sequence as usize % POOL_ENTRIES) * self.size;
        let entry = &mut self.bytes[start..start + self.size];
        write_tick(entry, sequence);
        entry
    }
}

/// Writes the tick for `sequence`: instrument id (u32), bid, ask and sequence (i64), little-endian.
pub fn write_tick(buffer: &mut [u8], sequence: i64) {
    if buffer.len() < TICK_LENGTH {
        return;
    }
    let bid = 100_000 + sequence % 1000;
    buffer[0..4].copy_from_slice(&((sequence % 100) as u32).to_le_bytes());
    buffer[4..12].copy_from_slice(&bid.to_le_bytes());
    buffer[12..20].copy_from_slice(&(bid + 10).to_le_bytes());
    buffer[20..28].copy_from_slice(&sequence.to_le_bytes());
}

/// Returns the sum of the four tick fields of `buffer`, or 0 for a shorter buffer.
pub fn read_tick(buffer: &[u8]) -> i64 {
    if buffer.len() < TICK_LENGTH {
        return 0;
    }
    let instrument_id = u32::from_le_bytes(buffer[0..4].try_into().unwrap()) as i64;
    let bid = i64::from_le_bytes(buffer[4..12].try_into().unwrap());
    let ask = i64::from_le_bytes(buffer[12..20].try_into().unwrap());
    let sequence = i64::from_le_bytes(buffer[20..28].try_into().unwrap());
    bid + instrument_id + ask + sequence
}

/// A fragment handler that counts messages and, when `decode` is set, sums their ticks.
pub struct Counter {
    pub count: i64,
    pub tick_sum: i64,
    decode: bool,
}

impl Counter {
    /// A counter that only counts.
    pub fn counting() -> Self {
        Counter {
            count: 0,
            tick_sum: 0,
            decode: false,
        }
    }

    /// A counter that counts and decodes every tick.
    pub fn decoding() -> Self {
        Counter {
            count: 0,
            tick_sum: 0,
            decode: true,
        }
    }
}

impl AeronFragmentHandlerCallback for Counter {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
        self.count += 1;
        if self.decode {
            self.tick_sum += read_tick(buffer);
        }
    }
}

/// Nanoseconds on the wall clock, for timestamps compared across processes.
pub fn realtime_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos() as i64)
        .unwrap_or(0)
}

/// The percentile `pct` of sorted nanosecond samples, in microseconds.
pub fn percentile_micros(sorted: &[i64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = ((pct / 100.0 * sorted.len() as f64) as usize).min(sorted.len() - 1);
    sorted[index] as f64 / 1000.0
}

/// The mean of nanosecond samples, in microseconds.
pub fn mean_micros(samples: &[i64]) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    samples.iter().map(|&sample| sample as f64).sum::<f64>() / samples.len() as f64 / 1000.0
}
