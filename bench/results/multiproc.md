# Multi-worker IPC throughput

## 256 B payload — aggregate msgs/sec

| client | N=1 | N=2 | N=4 | N=8 | N=12 | peak | scaling N=1→peak |
|---|---:|---:|---:|---:|---:|---:|---:|
| c | 71.76M | 134.57M | 181.26M | 173.40M | 150.77M | N=4 | 2.53x |
| cpp | 49.42M | 88.81M | 131.30M | 168.91M | 151.64M | N=8 | 3.42x |
| dotnet | 38.55M | 75.50M | 120.46M | 158.60M | 170.29M | N=12 | 4.42x |
| elixir | 9.63M | 19.74M | 38.17M | 74.10M | 86.22M | N=12 | 8.96x |
| go | 22.43M | 44.75M | 74.73M | 99.35M | 113.21M | N=12 | 5.05x |
| java | 36.85M | 65.79M | 108.74M | 156.53M | 156.05M | N=8 | 4.25x |
| python | 3.11M | 5.99M | 12.06M | 20.25M | 22.98M | N=12 | 7.40x |
| rust | 65.98M | 131.21M | 214.78M | 263.19M | 220.81M | N=8 | 3.99x |

## 32 B payload — aggregate msgs/sec

| client | N=1 | N=2 | N=4 | N=8 | N=12 | peak | scaling N=1→peak |
|---|---:|---:|---:|---:|---:|---:|---:|
| c | 97.84M | 196.60M | 307.94M | 480.20M | 499.95M | N=12 | 5.11x |
| cpp | 61.71M | 124.19M | 211.07M | 318.71M | 397.92M | N=12 | 6.45x |
| dotnet | 58.02M | 113.96M | 218.11M | 296.01M | 320.09M | N=12 | 5.52x |
| elixir | 10.25M | 19.65M | 41.02M | 74.56M | 98.02M | N=12 | 9.56x |
| go | 24.95M | 49.82M | 96.15M | 145.27M | 180.89M | N=12 | 7.25x |
| java | 60.79M | 104.50M | 160.32M | 324.21M | 352.07M | N=12 | 5.79x |
| python | 3.23M | 6.27M | 12.26M | 19.03M | 22.77M | N=12 | 7.05x |
| rust | 91.63M | 182.00M | 342.19M | 593.17M | 660.27M | N=12 | 7.21x |

## Method

- Host: Apple M3 Max, 16 logical cores (12 performance + 4 efficiency). `Darwin MacBook-Pro.local 27.0.0 Darwin Kernel Version 27.0.0: Tue Aug 11 21:05:42 PDT 2026; root:xnu-13432.1.9~1/RELEASE_ARM64_T6031 arm64`. OTP 28 / Elixir 1.19.5.
- Each worker owns one publication and one subscription on a private `aeron:ipc` channel (term-length 16777216) against the shared media driver at `/tmp/ae_drv`. Workers never share a log buffer, so the driver only does conductor work.
- Loop per worker: offer 1000 messages, polling the subscription (limit 1024) whenever the publication back-pressures, then poll once more; run for 5 s after 2 s warmup. Aggregate ops/sec = total sent across workers ÷ the slowest worker's elapsed time. The receive count is checked against the send count for every worker; a mismatch is flagged in the table.
- Per-message work is the same for every Aeron client: encode a 28-byte tick (u32 instrument id, i64 bid, i64 ask, i64 sequence) into the payload, offer it, and on receive decode the four fields and fold them into a running sum. Each cell prints its tick sum on stderr.
- Every message differs: each client builds the same pool of 4096 payloads up front from one splitmix64 byte stream (seed 0x5EEDAE20), and message `i` is pool entry `i mod 4096` with its tick stamped over the first 28 bytes, so every byte varies and the source is not always in L1.
- `elixir`: N BEAM processes in one VM sharing one `AeronElixir` client, direct handles (`AeronElixir.publication_handle/1`, `subscription_handles/1`), per-message `Publisher.publish/3`, drain with `Subscriber.collect/2` and decode every payload.
- `elixir_list`: same work as `elixir` (a distinct tick encoded per message, every payload decoded on receive), but the send side hands each batch of 1000 payloads to `Publisher.publish_list/3` as one native call. Equal work, amortized crossing.
- `beam`: N producer/consumer pairs of BEAM processes using `send`/`receive`, one tick per message, consumer decodes. No Aeron; reference for in-VM messaging.
- `python`: N OS processes via `multiprocessing` (spawn), one `pyaeron.Aeron` client each; per-message `offer` with the fragment handler decoding the tick fields.
- `java`: N threads sharing one `Aeron` client (1.49.3); per-message `offer` with a decoding `FragmentHandler`.
- `c`: N pthreads, one `aeron_t` client per thread (libaeron from ../aeron/build); per-message `aeron_publication_offer` with a decoding fragment handler.
- `cpp`: N `std::thread`s, one C++ wrapper `Aeron` client per thread; per-message `Publication::offer` with a decoding lambda handler passed to the template `Subscription::poll`.
- `go`: N goroutines, one aergo client per goroutine (pure Go, `github.com/andrewwormald/aergo` pinned in `bench/go/go.mod`); per-message `Publication.Offer` with a decoding fragment handler. aergo has no conductor thread, so each goroutine also calls `DoWork()` while waiting for its first message (image discovery) and once every 1024 operations (keepalives); nothing else is added to the measured loop.
- `dotnet`: N threads sharing one Aeron.NET client, as the Java threads do (`Aeron.Client` 1.52.2 from NuGet on .NET 10, pinned in `bench/dotnet/BenchKit/BenchKit.csproj`); per-message `Publication.Offer` with a decoding `FragmentHandler`.
- `rust`: N threads, one client per thread as in C (rusteron-client 0.2.10, Rust bindings over the Aeron 1.52.2 C client it builds from source (pinned in `bench/rust/Cargo.toml`)); per-message `offer` with a decoding fragment handler.

