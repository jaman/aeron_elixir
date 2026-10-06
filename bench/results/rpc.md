# Request/response round trip over `aeron:ipc`

One-at-a-time request and response: the pinger sends a 32-byte message and waits for its echo before sending the next. 100000 warmup round trips, then 1000000 measured round trips per pair.

## Single pair (two processes)

| client | p50 µs | p90 µs | p99 µs | p99.9 µs | max µs | mean µs | round trips/s |
|---|---:|---:|---:|---:|---:|---:|---:|
| c | 0.125 | 0.125 | 0.167 | 5.708 | 158.500 | 0.137 | 6.80M |
| cpp | 0.125 | 0.167 | 0.209 | 3.000 | 141.750 | 0.156 | 5.84M |
| rust | 0.125 | 0.167 | 0.208 | 2.917 | 157.250 | 0.150 | 5.78M |
| dotnet | 0.166 | 0.208 | 0.416 | 7.459 | 1053.333 | 0.187 | 4.95M |
| java | 0.167 | 0.209 | 0.292 | 6.125 | 204.291 | 0.202 | 4.09M |
| go | 0.208 | 0.209 | 0.333 | 9.292 | 127.167 | 0.221 | 3.97M |
| elixir | 0.250 | 0.292 | 0.417 | 10.667 | 113.708 | 0.300 | 2.89M |
| python | 0.708 | 0.791 | 1.000 | 7.500 | 129.625 | 0.745 | 1.26M |

## N concurrent pairs — aggregate round trips/s over the shared timed window and median per-pair p50

| client | N=1 rt/s | N=4 rt/s | N=12 rt/s | N=1 p50 µs | N=4 p50 µs | N=12 p50 µs |
|---|---:|---:|---:|---:|---:|---:|
| c | 6.80M | 17.69M | 14.49M | 0.125 | 0.125 | 0.166 |
| cpp | 5.84M | 16.17M | 12.66M | 0.125 | 0.166 | 0.167 |
| dotnet | 4.95M | 14.46M | 10.82M | 0.166 | 0.167 | 0.209 |
| elixir | 2.89M | 8.58M | 5.80M | 0.250 | 0.250 | 0.500 |
| go | 3.97M | 11.75M | 7.67M | 0.208 | 0.209 | 0.250 |
| java | 4.09M | 8.60M | 6.82M | 0.167 | 0.292 | 0.167 |
| python | 1.26M | 4.19M | 3.66M | 0.708 | 0.833 | 1.083 |
| rust | 5.78M | 16.36M | 11.16M | 0.125 | 0.125 | 0.208 |

## Method

- Host: Apple M3 Max, 16 logical cores (12 performance + 4 efficiency), macOS, no CPU pinning. Media driver: C `aeronmd_s` at `/tmp/ae_drv`, default threading, shared by every cell. OTP 28 / Elixir 1.19.5.
- Each pair uses two stream ids on `aeron:ipc` (ping and pong), unique per cell so lingering resources from a finished cell never collide with the next.
- Every pinger runs its warmup, then waits until the wall clock reaches a start time the runner sets 10 s after launching the pingers, so the timed loops of all N pairs begin together. Pingers sleep until 2 ms before that time and spin for the rest, so waiting pingers leave the CPU to the ones still warming up; with more busy processes than cores some still start late, and `latest_start_ms` in `rpc.json` records how late the last one began. A cell fails if the pairs' timed loops do not all overlap. Each pinger reports the wall-clock start and finish of its timed loop. Round trips/s is the total measured round trips of all pairs divided by the time from the first start to the last finish. Per-pair p50 for N pairs is the median of the pairs' p50 values.
- Every client is a program in this repository doing identical work, not an upstream sample. Per round trip the pinger encodes a 28-byte tick (little-endian u32 instrument id, i64 bid, i64 ask, i64 sequence) into the payload, offers it, spin-polls with a fragment limit of 1 until the echo arrives, and decodes the echo's four fields into a running sum. The ponger decodes every request the same way before echoing it back. Both sides print their sum on stderr so the decode cannot be optimized away.
- Every message differs: each client builds the same pool of 4096 payloads up front from one splitmix64 byte stream (seed 0x5EEDAE20), and message `i` is pool entry `i mod 4096` with its tick stamped over the first 28 bytes, so every byte varies and the source is not always in L1.
- `c`: `bench/c/rpc.c` (`bench_c_rpc --mode=ping|pong`), C client, busy-spin, two OS processes.
- `cpp`: `bench/cpp/rpc.cpp` (`bench_cpp_rpc --mode=ping|pong`), C++ wrapper API, busy-spin, two OS processes.
- `go`: `bench/go/cmd/rpc` (`rpc --mode=ping|pong`), one aergo client (pure Go, `github.com/andrewwormald/aergo` pinned in `bench/go/go.mod`). aergo has no conductor thread, so the program drives it: `DoWork()` while waiting for the first message (image discovery) and once every 1024 operations (keepalives); nothing else is added to the measured loop; busy-spin, two OS processes; the responder echoes the payload aergo copied out of the log.
- `dotnet`: `bench/dotnet/Rpc` (`Rpc --mode=ping|pong`), Aeron.NET (`Aeron.Client` 1.52.2 from NuGet, pinned in `bench/dotnet/BenchKit/BenchKit.csproj`) on .NET 10; busy-spin, two OS processes; the responder echoes the log buffer directly.
- `rust`: `bench/rust/src/bin/rpc.rs` (`rpc --mode=ping|pong`), rusteron-client 0.2.10, Rust bindings over the Aeron 1.52.2 C client it builds from source (pinned in `bench/rust/Cargo.toml`); busy-spin, two OS processes; the responder echoes the log buffer directly.
- `java`: `bench/java/Rpc.java` (`io.aeron.samples.bench.Rpc --mode=ping|pong`) against aeron-all 1.49.3, BusySpinIdleStrategy, two JVMs.
- `python`: `bench/python/rpc_ping.py` / `rpc_pong.py` with pyaeron (C client bindings), two OS processes, busy-spin; the pinger decodes the zero-copy `BufferView` with `struct.unpack_from`, the responder copies the payload out of the log (`tobytes`) before decoding and offering it back.
- `elixir`: `bench/elixir/rpc_ping.exs` / `rpc_pong.exs`, two BEAM VMs each with its own `AeronElixir` client, direct handles, `Publisher.publish/3` and `Subscriber.next/1`, busy-spin; ticks are built as iodata and decoded with a binary pattern match. Round-trip timings are written into a preallocated `:atomics` array so the measured loop does not grow the pinger's heap.
- `elixir_invm`: `bench/elixir/rpc_pair.exs`, pinger and ponger as two BEAM processes in one VM sharing one client (N pairs = 2N processes).
- `beam`: two BEAM processes using `send`/`receive`, no Aeron; the same tick is built and decoded on both sides. Reference for in-VM messaging, not an Aeron client.

