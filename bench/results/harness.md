# Market-data harness

A synthetic price publisher, a C program on the Aeron C client, runs as its own
OS process, and each client consumes from it in turn, one client at a time.
Every cell is run 3 times and the median run is shown: the
median consume rate flat out, the median p99 at a fixed rate. `harness.json`
keeps every run's p99 and consume rate (`p99_runs`, `consume_rate_runs`).

## Saturation — publisher flat out

| client | published | consumed | consume rate/s | lag | gaps | p50 µs | p99 µs | p99.9 µs | max µs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| elixir_direct | 94.74M | 94.74M | 9.47M | 0.00M | 0 | >10000 | >10000 | >10000 | >10000 |
| python | 27.12M | 27.12M | 2.71M | 0.00M | 0 | >10000 | >10000 | >10000 | >10000 |
| java | 261.04M | 261.04M | 26.10M | 0.00M | 0 | >10000 | >10000 | >10000 | >10000 |
| c | 320.86M | 320.86M | 32.09M | 0.00M | 0 | 0.05 | 9.05 | 24.05 | 86.0 |
| go | 248.99M | 248.99M | 24.90M | 0.00M | 0 | >10000 | >10000 | >10000 | >10000 |
| dotnet | 278.52M | 278.52M | 27.85M | 0.00M | 0 | 0.05 | 5371.05 | >10000 | >10000 |
| rust | 334.02M | 334.02M | 33.40M | 0.00M | 0 | 9059.05 | 9470.05 | >10000 | >10000 |

Latency under saturation is queue depth, not transport cost: the publisher
outruns the consumer, so each tick waits in the log before it is read. The
columns that answer "did it keep up" are consume rate and lag.

## Rate steps — latency below saturation

| client | 250k/s | 500k/s | 1000k/s | 2000k/s |
|---|---|---|---|---|
| elixir_direct | p50 0.05 p99 2.05 | p50 0.05 p99 2.05 | p50 0.05 p99 2.05 | p50 0.05 p99 3.05 |
| python | p50 0.05 p99 0.05 | p50 0.05 p99 0.05 | p50 0.05 p99 0.05 | p50 0.05 p99 4.05 |
| java | p50 0.55 p99 0.55 | p50 0.55 p99 0.55 | p50 0.55 p99 0.55 | p50 0.55 p99 0.55 |
| c | p50 0.05 p99 0.05 | p50 0.05 p99 0.05 | p50 0.05 p99 0.05 | p50 0.05 p99 0.05 |
| go | p50 0.05 p99 12.05 | p50 0.05 p99 12.05 | p50 0.05 p99 11.05 | p50 0.05 p99 11.05 |
| dotnet | p50 0.05 p99 0.05 | p50 0.05 p99 0.05 | p50 0.05 p99 0.05 | p50 0.05 p99 0.05 |
| rust | p50 0.05 p99 0.05 | p50 0.05 p99 0.05 | p50 0.05 p99 0.05 | p50 0.05 p99 0.05 |

Each cell is p50 / p99 in µs. A client is not run at a higher rate once it
fails to consume 98% of what was published at the rate below.


### Reading these numbers

- Flat out, a consumer that back-pressures the publisher is reading as fast as it can, so its consume rate is its own maximum. Highest first: `rust` 33.40M/s (publisher back-pressured for 2134 ms), `c` 32.09M/s (publisher back-pressured for 0 ms), `dotnet` 27.85M/s (publisher back-pressured for 84 ms), `java` 26.10M/s (publisher back-pressured for 3846 ms), `go` 24.90M/s (publisher back-pressured for 4119 ms), `elixir_direct` 9.47M/s (publisher back-pressured for 7764 ms), `python` 2.71M/s (publisher back-pressured for 9324 ms). 
- Lag is zero everywhere because IPC cannot drop. When a consumer falls behind,
  the publication limit stops the publisher rather than discarding messages, so
  a slow consumer shows up as a lower publish rate and a deeper queue, never as
  loss. A slow consumer's saturation latencies can exceed the histogram's 10 ms
  ceiling for that reason.
- Latencies are whole microseconds. Every side timestamps with CLOCK_REALTIME,
  which on macOS advances in 1 µs steps, so a p50 of 0.05 µs means the tick was
  read within the same microsecond it was stamped. Clients are separated only
  at 1 µs and above.
- `java` reads `Instant.now()`, which has the same 1 µs resolution, and adds half
  a step to remove the truncation bias, so where the others report 0.05 µs it
  reports 0.55 µs. The difference is that correction, not Java.

## Method

- Host: Apple M3 Max, 16 logical cores (12 performance + 4 efficiency), macOS, no CPU pinning. Media driver: C `aeronmd_s` at `/tmp/ae_drv`, default threading, shared by every cell.
- Channel `aeron:ipc?alias=prices|term-length=67108864`, one stream id per cell so a finished cell never collides with the next.
- The publisher is `bench/harness/price_server.c`. Flat out it offers ticks in batches of 200, one `aeron_publication_offer` per tick; at a fixed rate it publishes whatever the elapsed time says it is behind, at most 200 per step. It stamps each tick with `clock_gettime(CLOCK_REALTIME)` immediately before offering it, and counts the time it spends back-pressured.
- Each tick is 64 bytes little-endian: u32 instrument id, i64 bid, i64 ask, i64 sequence, i64 publish timestamp, then 28 bytes of padding taken from pool entry `sequence mod 4096` of the 64-byte payload pool every client's benches use (splitmix64, seed 0x5EEDAE20), so every byte of every tick varies. Prices random-walk per instrument across 500 instruments.
- Every consumer decodes all five fields of every tick and folds the prices into a running sum, printed to stderr so the decode cannot be optimised away.
- Each run lasts 10 s after the publisher and consumer are connected, and each cell is 3 runs on separate streams. The consumer exits once the stream has been idle for 750 ms.
- Timestamps are CLOCK_REALTIME on every side: `:os.system_time(:nanosecond)` in Elixir, `clock_gettime(CLOCK_REALTIME)` in C, `time.time_ns()` in Python, `time.Now().UnixNano()` in Go, `SystemTime::now()` in Rust, `clock_gettime(CLOCK_REALTIME)` in .NET, and `Instant.now()` in Java.
- Latency is recorded in a 100 ns-bucket histogram covering 0–10 ms; percentiles are bucket midpoints. The timestamps themselves are 1 µs steps on macOS (see above).
- `elixir_direct` polls through `AeronElixir.LogBuffer.Subscriber.collect/2` with a direct image handle.
