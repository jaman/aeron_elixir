# AeronElixir

Native Elixir client for the [Aeron](https://github.com/aeron-io/aeron) messaging
system. It bundles the Aeron C media driver, which the application starts and
supervises, or connects to a driver that something else runs. It talks to the
driver over the driver's shared-memory files, so an Elixir node can publish to
and subscribe from the same streams as Java, C, C++ and Python Aeron clients.

**Website:** [jaman.github.io/aeron_elixir](https://jaman.github.io/aeron_elixir/), with the
benchmarks, runnable examples and the pyaeron API mapping.

## Architecture

```
caller process ──publish/poll──► NIF (C) ───atomics──► mapped log buffers ◄── media driver
       │                                                        ▲
       └──add_publication / add_subscription──► ClientConductor ─┘  (CnC file, handshake)
```

- **`AeronElixir.NIF`** — the only native code in the client, a C NIF
  (`c_src/aeron_elixir_nif.c`, loaded by `AeronElixir.NIF`). Maps the
  CnC file and log buffers, provides atomic reads and writes, appends data frames
  (`offer`, `offer_n`, `offer_list`) and reads messages back with fragment
  reassembly (`poll_collect`, `poll_collect_framed`, `peek_collect_framed`,
  `poll_count`). `mix compile` builds it with `-O3`.
- **`AeronElixir.ClientConductor`** — one `GenServer` per client. Waits for a
  live driver, maps `cnc.dat`, sends the add and remove commands over the
  to-driver ring buffer, reads driver responses from the to-clients broadcast
  buffer, maps the log file the driver names, sends keepalives and checks the
  driver's heartbeat, tracks images as they arrive and leave, and invokes the
  image callbacks. When the driver is lost it closes every publication and image
  and stops. It is not on the message path.
- **`AeronElixir.RuntimeHandles`** — a public ETS table owned by each conductor
  holding `%LogBuffer.Publisher{}` and `%LogBuffer.Subscriber{}` handles. Every
  publish and poll reads its handle from this table and calls the NIF from the
  calling process, with no message to the conductor.
- **`AeronElixir.BatchPublisher`** — a process that owns one publication and
  sends everything queued in its mailbox as one native call each time it runs.
- **`AeronElixir.MediaDriver`** — runs a C media driver as a Port owned by an
  Elixir process: the bundled one by default, or any `aeronmd` binary.
- **`AeronElixir.DriverConfig`** — decides which driver the application uses:
  the bundled driver, started under the application's supervisor, or a driver
  named by `AERON_DIR` or `config :aeron_elixir, :aeron_dir`.
- **Bundled driver** — `mix compile` (`Mix.Tasks.Compile.AeronNative`, which
  also builds the NIF) builds the Aeron 1.51.0 C client and driver into
  `priv/bin/aeronmd`. The first build downloads the 1.51.0 release archive from
  GitHub, checks its pinned SHA-512 and unpacks the C sources into
  `_build/aeron-1.51.0`; later builds reuse them. Set `AERON_SOURCE_DIR` to an
  unpacked Aeron 1.51.0 tree to build without network access.
- **`AeronElixir.Resources.*`** — Ash resources (`Client`, `Publication`,
  `Subscription`, `Image`, `Counter`) backed by ETS. Lifecycle (connect, add,
  close) goes through Ash actions; the action changes call the conductor.
- **`AeronElixir.Protocol.*`** and **`AeronElixir.ChannelUri`** — pure Elixir
  encoders and decoders for the control protocol, the 32-byte data frame header
  and channel URIs, byte-matched to the Aeron Java flyweights, plus a validated
  URI builder.

## Public API

All functions live in `AeronElixir` unless a module is named. `publication`
means a `%Resources.Publication{}` record or a `%LogBuffer.Publisher{}` handle;
`subscription` a `%Resources.Subscription{}` record; `image` a
`%LogBuffer.Subscriber{}` handle.

Client and lifecycle:

| function | contract |
|---|---|
| `start_link(aeron_directory: dir)` | Starts a client against the driver at `dir`; returns the client process. The client lives as long as that process: when it exits, for any reason, the client is closed. |
| `add_publication(client, channel, stream_id)` | Driver handshake; returns `{:ok, publication}` with the driver-assigned session and geometry. |
| `add_exclusive_publication(client, channel, stream_id)` | Same, with a session only this client appends to; the record has `is_exclusive: true`. |
| `add_subscription(client, channel, stream_id, opts)` | Options `available_image:` and `unavailable_image:` take 1-arity functions called with `%{session_id, correlation_id, subscription_registration_id, source_identity}` when a publisher's image joins or leaves. |
| `close/1` | Closes a publication, subscription, counter or client; the driver is told to remove it. |
| `closed?/1` | Whether the record has been closed. |
| `connected?/1` | A publication with at least one connected subscriber, or a subscription with at least one image. |
| `await_connected(resource, timeout_ms \\ 5_000)` | Polls `connected?/1` every millisecond; `:ok` or `{:error, :timeout}`. |
| `position/1`, `position_limit/1` | A publication's current stream position and the position it may append to before back-pressure. |

Publishing:

| function | contract |
|---|---|
| `publish(publication, iodata)` | One message. Retries while back-pressured, not yet connected or rotating until the client's `driver_timeout_ms`, then returns the last status. `{:ok, position}`, `{:error, :back_pressured \| :not_connected \| :max_position_exceeded \| :closed}`. |
| `try_publish(publication, iodata)` | One attempt, no retry. Adds `{:error, :admin_action}` (a term rotation happened; call again). |
| `publish_list(publication, [iodata])` | Each element is one message, appended in order in a single native call. `{:ok, appended}`; `appended < length` means back-pressure part way, resume from `Enum.drop(list, appended)`. |
| `publish_n(publication, binary, count)` | `count` copies of one payload in a single native call, same return shape. |
| `BatchPublisher` | `start_link(publication: p)`, then `publish(server, iodata)` is asynchronous and never blocks: every wake-up drains the mailbox and sends it as one `publish_list`. Back-pressured messages stay queued and are retried on a 1 ms timer; nothing is dropped. `flush/1` blocks until the queue is empty; `pending/1` reports what is queued. |
| `publication_handle(publication)` | The `%LogBuffer.Publisher{}` handle, usable from any process without the per-call lookup. |

Payloads are binaries or iodata; iodata segments are written straight into the
frame without flattening. Messages longer than `max_payload_length` are
fragmented on publish and reassembled on poll.

Polling:

| function | contract |
|---|---|
| `poll(subscription, fragment_limit \\ 10, handler)` | Calls `handler.(payload, header)` once per whole message across all images, fragments reassembled; returns the message count. `header` is an encoded `AeronElixir.Header`, read field by field with its functions or a binary match. |
| `controlled_poll(subscription, fragment_limit \\ 10, handler)` | Like `poll/3`, honouring the handler's return: `:continue` (position released at the end), `:commit` (released at once), `:break` (stop after this message), `:abort` (stop before it; it is delivered again next time). |
| `poll_batch(subscription, fragment_limit \\ 64)` | `{:ok, count, payloads}` with no handler call: scan, reassembly, copy and batching happen in one native call. |
| `images/1`, `image_count/1`, `image_by_session_id/2` | The images (one per publisher session) attached to a subscription; `image_by_session_id/2` returns `{:ok, image}` or `{:error, :unknown_session}`. |
| `poll_image(image, fragment_limit \\ 10, handler)`, `poll_image_batch(image, fragment_limit \\ 64)` | `poll/3` and `poll_batch/2` for one publisher session. |
| `image_position/1`, `end_of_stream?/1` | The subscriber position of an image, and whether its publisher has ended the stream. |
| `subscription_handles/1` | The images as handles for processes that own the polling; re-fetch to pick up publishers that join later. |

`%Protocol.Frame{}` carries `frame_length`, `version`, `flags`, `type`,
`term_offset`, `session_id`, `stream_id`, `term_id`, `reserved_value`,
`initial_term_id` and `position` (the stream position after this frame).

Channels, idling, driver:

| module | contract |
|---|---|
| `ChannelUri.ipc(params)`, `ChannelUri.udp(params)` | Build `aeron:ipc?…` / `aeron:udp?…` strings from keyword parameters (`endpoint`, `interface`, `control`, `term_length`, `mtu`, `alias`, `session_id`, `reliable`, `linger`, `sparse`, `tags`, …), validating names and values; `term_length` must be a power of two between 64 KiB and 1 GiB. Returns the string or `{:error, reason}`. |
| `Idle` | `:busy_spin`, `:yield`, `{:sleep, ms}` or `Idle.backoff/1`; `Idle.idle(strategy, work_count)` returns the strategy for the next iteration. |
| `MediaDriver` | `start_link(aeron_dir: dir)` spawns the bundled driver (or `binary: path`), waits for `cnc.dat` and liveness, and stops the driver when the owner exits; `remove_directory: true` removes the directory afterwards. Refuses a directory a live driver already serves. Usable as a child spec for extra drivers; `stop/1`, `alive?/1`, `os_pid/1`, `aeron_dir/1`. |
| `DriverConfig` | `resolve/0` returns the mode (`:embedded` or `:external`), directory and binary in use; `aeron_dir/0`, `bundled_binary/0`. `mix aeron.driver` prints them. |

Counters: `add_counter/4`, `increment_counter/2`, `counter_value/1`.

Publications are also exposed as Ash actions on the resources
(`AeronElixir.Resources.Publication.publish`, `Subscription.poll`, `Image.poll`)
for callers that work through the Ash domain.

## Hot-path contract

- `publish/2`, `try_publish/2`, `publish_list/2` and `publish_n/3` are safe to
  call from many processes on the same publication. Term space is reserved with
  an atomic add on the term tail, and term rotation is guarded by a CAS, matching
  Aeron's concurrent `Publication`.
- Only one process may poll a given subscription or image at a time. The
  subscriber position counter is single-reader.
- Every payload returned by one `poll_batch/2`, `poll_image_batch/2`, `poll/3`
  or `controlled_poll/3` call is a sub-binary of a single allocation for that
  call, except that a `poll_batch/2` or `poll_image_batch/2` call returning one
  payload of 64 bytes or less returns it as a binary of its own. Holding one
  payload of a batch keeps the whole batch alive; `:binary.copy/1` any payload
  kept beyond the call.
- A message whose fragments are not all published yet is left for a later poll.
- Nothing on the message path sends a message to the conductor or any other
  process.
- Once a publication or image is closed, by `close/1` or because its client lost
  the driver, publishing returns `{:error, :closed}` and polling reads nothing.
  A handle keeps its mappings alive for as long as it is reachable, so using a
  closed handle is always safe.

## Requirements

- Elixir 1.19+ / OTP 28
- A C compiler: `cc`, or the one `CC` names. On macOS that is the Xcode
  Command Line Tools (`xcode-select --install`); on Linux, gcc or clang. No
  CMake or separately installed media driver is required.

## Build

```bash
mix deps.get
mix compile
```

`mix compile` builds the NIF into `priv/lib/aeron_elixir_nif.so` and the media
driver into `priv/bin/aeronmd`, each only when one of its sources has changed.

## Usage

The application starts the bundled media driver and supervises it, so a client
needs no configuration. From an application or `iex -S mix`:

```elixir
{:ok, client} = AeronElixir.start_link()

channel = AeronElixir.ChannelUri.ipc(alias: "orders", term_length: 16_777_216)
{:ok, publication} = AeronElixir.add_publication(client, channel, 1001)
{:ok, subscription} = AeronElixir.add_subscription(client, channel, 1001)
:ok = AeronElixir.await_connected(publication)

{:ok, _position} = AeronElixir.publish(publication, ["order:", "42"])

handler = fn payload, header ->
  IO.inspect({AeronElixir.Header.session_id(header), AeronElixir.Header.position(header), payload})
end

1 = AeronElixir.poll(subscription, 10, handler)

{:ok, 3} = AeronElixir.publish_list(publication, ["a", "b", "c"])
{:ok, 3, ["a", "b", "c"]} = AeronElixir.poll_batch(subscription, 64)

AeronElixir.close(publication)
AeronElixir.close(subscription)
AeronElixir.close(AeronElixir.client(client))
```

For the lowest per-message cost, fetch the handles once and use them from the
processes that own the work:

```elixir
{:ok, handle} = AeronElixir.publication_handle(publication)
{:ok, images} = AeronElixir.subscription_handles(subscription)

{:ok, _position} = AeronElixir.publish(handle, "direct")
{:ok, 1, ["direct"]} = AeronElixir.poll_batch(hd(images), 64)
```

To let many processes publish without each paying a native call, route them
through a `BatchPublisher`:

```elixir
{:ok, batcher} = AeronElixir.BatchPublisher.start_link(publication: publication)
:ok = AeronElixir.BatchPublisher.publish(batcher, "fire and forget")
:ok = AeronElixir.BatchPublisher.flush(batcher)
```

## Media driver

Which driver the application uses, in order of precedence:

| setting | behaviour |
|---|---|
| `AERON_DIR=/path` | connect to the driver something else runs at `/path`; nothing is started |
| `config :aeron_elixir, aeron_dir: "/path"` | the same, from configuration |
| neither | start the bundled driver, supervised, in a directory private to this running application |

`config :aeron_elixir, driver: :embedded` with a directory makes this
application start and own the driver at a shared location, for other
applications on the host to connect to. `config :aeron_elixir, binary: path`
launches another `aeronmd` instead of the bundled one.

The private directory is `$XDG_RUNTIME_DIR/aeron_elixir/os-<pid>`, or under
`/dev/shm` or the system temp directory when that is unset: memory-backed,
per-user runtime storage. The driver recreates its files on every start and
deletes them when it stops; directories left by a VM that died are swept on
the next start. If the VM dies without stopping the driver, the bundled driver
notices and shuts itself down.

`mix aeron.driver` prints the mode, the directory and the binary with its
version, so another program can run the same binary or connect to the same
directory.

Extra drivers, for example one per tenant, are ordinary children:

```elixir
{AeronElixir.MediaDriver, aeron_dir: "/run/aeron/tenant_a", name: MyApp.TenantADriver}
```

### Losing the driver

The client follows the Aeron client contract. Every keepalive interval the
conductor reads the driver's heartbeat. When the driver has shut down, has not
heartbeated for `driver_timeout_ms` (default 5000, a `start_link/1` option), or
reports that this client timed out, the conductor closes every publication and
image (publishing returns `{:error, :closed}`, polling reads nothing,
`unavailable_image` handlers run) and stops with `{:shutdown, :driver_shutdown}`,
`{:shutdown, :driver_timeout}` or `{:shutdown, :client_timeout}`. The client is
never reconnected in place: a restarted driver has new buffers and positions, so
messages in flight are gone, and recreating a publication is a new session.

The process started by `start_link/1` exits with that reason. Supervise it, and
put the processes that own publications and subscriptions after it in a
`:rest_for_one` supervisor, so a lost driver restarts them in order and they add
their publications and subscriptions again; connecting waits for the driver:

```elixir
children = [
  {AeronElixir, name: MyApp.Aeron},
  MyApp.OrderPublisher
]

Supervisor.start_link(children, strategy: :rest_for_one)
```

The embedded driver is supervised the same way: if it exits, the application
restarts it and then its clients.

## Examples

`examples/` holds runnable scripts. Each uses the driver the application
selects, so they run with no setup; `10_lifecycle.exs` also starts and stops a
second driver of its own.

```bash
mix run examples/01_ipc_producer_consumer.exs
mix run examples/10_lifecycle.exs
```

| script | shows |
|---|---|
| `01_ipc_producer_consumer.exs` | one publication, one subscription, `publish/2`, `poll_batch/2` |
| `02_ipc_fan_out.exs` | one publication delivered to two subscriptions |
| `03_rpc.exs` | request and response streams with a correlation id in the payload |
| `04_offer_status.exs` | `try_publish/2` statuses, `position/1`, `position_limit/1` |
| `05_polling.exs` | poll duty cycle with `Idle`, fragment limits, reassembled fragments, `header.position` |
| `06_channels.exs` | `ChannelUri.ipc/1`, `ChannelUri.udp/1`, validation, `Protocol.URI.parse/1` |
| `07_streams.exs` | independent stream ids on one channel |
| `08_exclusive_publication.exs` | `add_exclusive_publication/3`, distinct sessions, `image_count/1` |
| `09_iodata.exs` | binaries and nested iodata published without flattening |
| `10_lifecycle.exs` | `MediaDriver`, `connected?/1`, `close/1`, `closed?/1` |
| `11_images.exs` | image callbacks, `image_by_session_id/2`, `poll_image/3`, `image_position/1`, `end_of_stream?/1` |
| `12_udp.exs` | `ChannelUri.udp/1` on a loopback endpoint, publish and poll over UDP |

`examples/README.md` maps each script and each API call to its
[pyaeron](https://pypi.org/project/pyaeron/) equivalent.

### Coverage against upstream samples

The reference examples for any Aeron client are the samples in the Aeron
repository (`aeron-samples/src/main/java/io/aeron/samples` and
`aeron-samples/src/main/c`). What each needs, and whether this client provides
it:

| sample | verdict | with |
|---|---|---|
| `BasicPublisher`, `SimplePublisher`, `basic_publisher.c` | expressible | `add_publication/3`, `publish/2` or `try_publish/2` with `Idle` on back-pressure |
| `BasicSubscriber`, `SimpleSubscriber`, `basic_subscriber.c` | expressible | `add_subscription/4`, `poll/3` with the frame header, `Idle` |
| `StreamingPublisher`, `streaming_publisher.c`, `streaming_exclusive_publisher.c` | expressible | `publish_list/2` or `publish/2` in a loop, `add_exclusive_publication/3` |
| `RateSubscriber`, `rate_subscriber.c`, `ImageRateSubscriber` | expressible | `poll_batch/2` or `poll_image_batch/2` with a timer for the rate |
| `MultipleSubscribersWithFragmentAssembly`, `MultiplePublishersWithFragmentation` | expressible | two stream ids on one channel; fragmentation and reassembly are built in |
| `Ping`, `Pong`, `cping.c`, `cpong.c`, `EmbeddedPingPong` | expressible | two streams, `poll_batch/2` with limit 1, `MediaDriver` for the embedded variant; `bench/elixir/rpc_*.exs` are the equivalent programs |
| `EmbeddedIpcThroughput`, `EmbeddedExclusiveIpcThroughput`, `EmbeddedThroughput` | expressible | `MediaDriver`, `publish_list/2`, `poll_batch/2`; `bench/` holds the equivalent measurements |
| `EmbeddedBufferClaimIpcThroughput`, `EmbeddedExclusiveBufferClaimIpcThroughput`, `FileSender` (its `tryClaim` path) | partial | no `try_claim`; iodata `publish/2` writes without an intermediate copy but the caller cannot fill the frame in place |
| `EmbeddedExclusiveVectoredIpcThroughput` | expressible | vectored offers are iodata |
| `FileSender`, `FileReceiver` | expressible | chunked messages with a header as iodata, `poll/3` reassembling per header |
| `EmbeddedDualExclusiveThroughput`, `StressMdcClient`, `StressMdcServer`, `basic_mds_subscriber.c` | not | multi-destination cast and multi-destination subscription (`addDestination`, `control-mode=manual`) need the ADD_DESTINATION and REMOVE_DESTINATION commands and the UDP path |
| `EmbeddedExclusiveSpiedThroughput` | not | no `aeron-spy:` channel support |
| `response_client.c`, `response_server.c`, `echo/*` | not | no `response-endpoint` or `response-correlation-id` handling in the conductor |
| `StressUnicastClient`, `StressUnicastServer`, `raw/*` | expressible | `ChannelUri.udp/1` with an endpoint; the raw samples bypass the driver and have no client equivalent |
| `AeronStat`, `StreamStat`, `BacklogStat`, `aeron_stat.c` | partial | the CnC counters buffers are mapped (`CnC.Reader`) and single counters are readable, but there is no reader that iterates every driver counter with its key and label |
| `ErrorStat`, `error_stat.c`, `LossStat`, `loss_stat.c` | not | the error log buffer is mapped but not decoded; the loss report file is not read |
| `CncFileReader`, `DriverTool`, `driver_tool.c`, `LogInspector` | partial | CnC metadata and the to-driver and to-clients buffers are read for the handshake; no standalone inspection tool |
| `archive/*`, `cluster/*` | not | no Archive or Cluster client |
| `security/*` | not | no authentication or authorisation hooks in the handshake |

Counting the rows: 10 expressible, 3 partial, 6 not.

## Testing

```bash
mix test
```

Tags: `:system` (uses the application's driver: the bundled one, or the one
`AERON_DIR` names), `:interop`, `:protocol`. `mix quality` runs the formatter,
Credo (strict) and Dialyzer.

## Benchmarks

Four harnesses, all over `aeron:ipc` against one C media driver that every
client shares, each writing its results under `bench/results/`. Start that
driver first, for example `AERON_DIR=/tmp/ae_drv ../aeron/build/binaries/aeronmd_s`;
with `AERON_DIR` set, the Elixir clients connect to it instead of starting the
bundled driver.

- `bench/run.exs` — single process: latency (offer → poll round trip, one
  message at a time) and throughput (batches of 1000 with drain) per client and
  payload size; `summary.{csv,json,html}`.
- `bench/run_multiproc.exs` — N workers per client, each with a private
  publication and subscription pair; `multiproc.md`.
- `bench/run_rpc.exs` — request/response round trip between two processes, 32
  bytes, one request in flight; `rpc.md`.
- `bench/harness/run.exs` — market data: a C price publisher
  (`bench/harness/price_server.c`) in its own process, each client consuming its
  ticks in turn; `harness.md`.

```bash
AERON_DIR=/tmp/ae_drv mix run bench/run.exs
AERON_DIR=/tmp/ae_drv mix run bench/run.exs clients=elixir_raw,python sizes=32,256 time=5
AERON_DIR=/tmp/ae_drv mix run bench/run_multiproc.exs workers=1,2,4,8,12 sizes=256,32
AERON_DIR=/tmp/ae_drv mix run bench/run_rpc.exs
AERON_DIR=/tmp/ae_drv mix run bench/harness/run.exs
```

`merge=true` keeps the stored results of clients a run does not include, and
`clients=` with no clients re-renders the stored results without running
anything.

Per message, every Aeron client does the same work in every harness: stamp a
28-byte tick (instrument id, bid, ask, sequence) into the payload, offer it, and
on receive decode the four fields into a running sum. Every client is a program
in this repository rather than an upstream sample.

Every message differs. Each client builds the same pool of 4096 payloads before
any timing starts, cut from one splitmix64 byte stream (seed `0x5EEDAE20`,
outputs written little-endian), and message `i` is pool entry `i mod 4096` with
its tick stamped over the first 28 bytes. The C, C++, Java, Python, Elixir, Go,
.NET and Rust implementations build byte-identical pools (`bench/c/payload_pool.h`,
`bench/cpp/payload_pool.hpp`, `bench/java/PayloadPool.java`,
`bench/python/payload_pool.py`, `AeronElixir.Bench.Shared.payload_pool/1`,
`benchkit.NewPool`, `bench/dotnet/BenchKit/Kit.cs`, `bench/rust/src/lib.rs`).

| client | what it measures |
|---|---|
| `elixir_raw` (single process), `elixir` (multi-worker, request/response), `elixir_direct` (market data) | aeron_elixir through `%LogBuffer.Publisher{}` and `%LogBuffer.Subscriber{}` handles, one message per publish call; `Subscriber.next/1` for one message at a time, `Subscriber.collect/2` for batches |
| `python` | [pyaeron](https://pypi.org/project/pyaeron/) over the Aeron C client |
| `java` | Aeron Java client |
| `c` | Aeron C client |
| `cpp` | Aeron C++ wrapper |
| `go` | [aergo](https://github.com/andrewwormald/aergo), a pure Go client; one client per process or worker goroutine |
| `dotnet` | [Aeron.NET](https://github.com/AdaptiveConsulting/Aeron.NET) 1.52.2 on .NET 10, Adaptive's .NET port of the Java client; threads share one client |
| `rust` | [rusteron-client](https://lib.rs/crates/rusteron-client) 0.2.10, Rust bindings over the Aeron 1.52.2 C client, which it builds from source; one client per worker thread |

Other variants run when named in `clients=`: `elixir` in the single-process
harness (the record functions `publish/2` and `poll/3`), `elixir_list`
(`publish_list/2`, 1000 messages per call), `elixir_invm` (request/response with
both sides in one VM), `elixir_public` (market data through `poll/3`) and `beam`
(BEAM `send`/`receive`, no Aeron).

Prerequisites per client: `c`/`cpp` need `../aeron` built with CMake
(`bench/c` and `bench/cpp` link against `../aeron/build/lib`; `bench/c` also
builds the market-data publisher); `java` needs `aeron-all-1.49.3.jar` in
`~/.m2`; `python` needs `bench/python/.venv` with `pyaeron` installed
(`uv venv --python 3.14 bench/python/.venv && uv pip install --python
bench/python/.venv/bin/python pyaeron`); `go` needs the binaries built in
`bench/go` (`cd bench/go && go build -o build/ ./cmd/...`), with Go 1.27 pinned
for that directory in `bench/go/mise.toml` and aergo pinned in
`bench/go/go.mod`. `dotnet` needs the .NET 10 SDK (Homebrew `dotnet`, or
`mise use dotnet@10` in `bench/dotnet`; `bench/dotnet/global.json` pins the SDK
major) and the four programs published into `bench/dotnet/build`:
`cd bench/dotnet && for p in Bench BenchMultiproc Rpc Consumer; do dotnet publish $p/$p.csproj -c Release -o build; done`.
The runners start them with `dotnet <Program>.dll`, and NuGet fetches
`Aeron.Client` 1.52.2 on the first build. `rust` needs Rust 1.91 (mise) and
CMake, because rusteron-client compiles the Aeron C client: `cd bench/rust &&
cargo build --release` builds the four programs into `bench/rust/target/release`.

aergo has no conductor thread: the caller runs its conductor with `DoWork()`.
The Go programs call it while waiting for the first message, which is when the
image is discovered, and once every 1024 operations for keepalives. Apart from
that, each Go program does exactly what the C program for the same harness does.

All results below come from one run on an Apple M3 Max (12 performance and 4
efficiency cores), macOS 27, OTP 28 and Elixir 1.19.5, with one C media driver
on default threading shared by every cell and no CPU pinning. The machine is a
desktop with other applications running, and multi-worker results in particular
vary between runs by tens of percent. The `rust` rows were measured in a later
session than the others.

### Single process

Median round trip, µs (offer then poll on one thread):

| client | 32 B | 256 B | 1024 B |
|---|---|---|---|
| c | 0.041 | 0.041 | 0.042 |
| dotnet | 0.041 | 0.042 | 0.042 |
| rust | 0.041 | 0.042 | 0.042 |
| cpp | 0.042 | 0.042 | 0.042 |
| java | 0.042 | 0.042 | 0.042 |
| go | 0.083 | 0.083 | 0.084 |
| elixir_raw | 0.125 | 0.167 | 0.167 |
| python | 0.292 | 0.292 | 0.333 |

C, C++, Java, .NET and Rust share the lowest median, 0.041–0.042 µs; Go follows at
0.083 µs and aeron_elixir at 0.125 µs, ahead of Python's 0.292 µs. Every client
stamps a fresh tick into each message inside the timed window. Full percentiles
for every client are in the round-trip section below.

Throughput, million messages/s:

| client | 32 B | 256 B | 1024 B |
|---|---|---|---|
| c | 98.7 | 72.7 | 32.7 |
| rust | 94.1 | 68.7 | 32.0 |
| java | 60.2 | 30.2 | 23.4 |
| cpp | 60.0 | 48.0 | 26.5 |
| dotnet | 55.9 | 37.0 | 22.5 |
| go | 25.6 | 22.9 | 16.5 |
| elixir_raw | 9.7 | 6.0 | 4.1 |
| python | 3.2 | 3.1 | 2.9 |

### Varied payloads

Every message in every harness differs (see above). Measured on its own, by
building the previous identical-payload C and C++ programs from source and
alternating them with the current ones at 32 B: the varied pool costs the C
client about 11% of its throughput (112.2 M to 99.7 M messages/s, and 36.7 M to
32.7 M at 1024 B), while the C++ client is unchanged (61.2 M against 61.0 M). In
the multi-worker harness at 256 B each worker reads its own 1 MB pool, so twelve
workers stream 12 MB of distinct payloads.

### Multi-worker

Aggregate million messages/s for N workers, each with its own publication and
subscription pair. Elixir workers are BEAM processes in one VM sharing one
client; Java and .NET threads share one client; C and C++ use one client per
thread, as does Rust; Go uses one aergo client per goroutine; Python uses one OS
process and one client per worker.

256 B:

| client | N=1 | N=2 | N=4 | N=8 | N=12 | peak | N=1→peak |
|---|---|---|---|---|---|---|---|
| rust | 65.98 | 131.21 | 214.78 | 263.19 | 220.81 | N=8 | 3.99x |
| dotnet | 38.55 | 75.50 | 120.46 | 158.60 | 170.29 | N=12 | 4.42x |
| java | 36.85 | 65.79 | 108.74 | 156.53 | 156.05 | N=8 | 4.25x |
| cpp | 49.42 | 88.81 | 131.30 | 168.91 | 151.64 | N=8 | 3.42x |
| c | 71.76 | 134.57 | 181.26 | 173.40 | 150.77 | N=4 | 2.53x |
| go | 22.43 | 44.75 | 74.73 | 99.35 | 113.21 | N=12 | 5.05x |
| elixir | 9.63 | 19.74 | 38.17 | 74.10 | 86.22 | N=12 | 8.96x |
| python | 3.11 | 5.99 | 12.06 | 20.25 | 22.98 | N=12 | 7.40x |

32 B:

| client | N=1 | N=2 | N=4 | N=8 | N=12 | peak | N=1→peak |
|---|---|---|---|---|---|---|---|
| rust | 91.63 | 182.00 | 342.19 | 593.17 | 660.27 | N=12 | 7.21x |
| c | 97.84 | 196.60 | 307.94 | 480.20 | 499.95 | N=12 | 5.11x |
| cpp | 61.71 | 124.19 | 211.07 | 318.71 | 397.92 | N=12 | 6.45x |
| java | 60.79 | 104.50 | 160.32 | 324.21 | 352.07 | N=12 | 5.79x |
| dotnet | 58.02 | 113.96 | 218.11 | 296.01 | 320.09 | N=12 | 5.52x |
| go | 24.95 | 49.82 | 96.15 | 145.27 | 180.89 | N=12 | 7.25x |
| elixir | 10.25 | 19.65 | 41.02 | 74.56 | 98.02 | N=12 | 9.56x |
| python | 3.23 | 6.27 | 12.26 | 19.03 | 22.77 | N=12 | 7.05x |

aeron_elixir scales furthest from one worker to twelve: 8.96x at 256 B and
9.56x at 32 B, against 4.4x for .NET and 2.5x for C at 256 B. At 256 B C peaks
at four workers and C++, Java and Rust at eight, while .NET, Go, Elixir and Python
keep rising to twelve. Absolute throughput still favours the native clients: at
twelve workers C carries 1.7 times aeron_elixir at 256 B and 5.1 times at 32 B.
Rust and C drive the same Aeron C client, so the gap between them here (Rust
660 M messages/s against C's 500 M at 32 B and twelve workers) is the run-to-run
variance described above, not a difference between the clients.

### NIF call cost

A NIF call costs the same whatever else the node is doing.
`bench/elixir/nif_ceiling.exs` measures a trivial C `add/2` NIF, aeron_elixir's
own `read_int64/1` and a pure BIF across a rising number of concurrently calling
processes. At twelve callers the C NIF costs 11.6 ns, `read_int64/1` 14.9 ns and
the BIF 12.0 ns. The figures stay between 10 and 17 ns from one to twenty
callers, and twelve callers together make over 1,000 million `add/2` calls per
second. `:crypto.hash/2` rises from 121 ns to 2,825 ns over the same caller
counts and its aggregate throughput falls; that is contention inside crypto and
says nothing about the NIF call path.

### Request/response round trip

32-byte messages, one request in flight, 100,000 warmup round trips then
1,000,000 measured. Both sides encode and decode the tick: the pinger stamps it
into the next pool entry, offers it, spin-polls for the echo and decodes the
echo's four fields; the ponger decodes every request before echoing it back.
Both print their accumulated sum so the decode cannot be optimized away. Each
pair is two OS processes.

| client | p50 µs | p90 µs | p99 µs | p99.9 µs | max µs | mean µs | round trips/s |
|---|---|---|---|---|---|---|---|
| c | 0.125 | 0.125 | 0.167 | 5.708 | 158.500 | 0.137 | 6.80 M |
| rust | 0.125 | 0.167 | 0.208 | 2.917 | 157.250 | 0.150 | 5.78 M |
| cpp | 0.125 | 0.167 | 0.209 | 3.000 | 141.750 | 0.156 | 5.84 M |
| dotnet | 0.166 | 0.208 | 0.416 | 7.459 | 1053.333 | 0.187 | 4.95 M |
| java | 0.167 | 0.209 | 0.292 | 6.125 | 204.291 | 0.202 | 4.09 M |
| go | 0.208 | 0.209 | 0.333 | 9.292 | 127.167 | 0.221 | 3.97 M |
| elixir | 0.250 | 0.292 | 0.417 | 10.667 | 113.708 | 0.300 | 2.89 M |
| python | 0.708 | 0.791 | 1.000 | 7.500 | 129.625 | 0.745 | 1.26 M |

Round trips/s is the measured round trips divided by the wall-clock time of the
timed loop. aeron_elixir's median, 0.250 µs, is twice the C, C++ and Rust
clients' 0.125 µs and 1.5 times .NET's and Java's; its p99.9 is the widest of
the Aeron clients.

Three differences in the responders are properties of the clients rather than
the harness. Python copies the payload with `tobytes()` before echoing, because
a pyaeron `BufferView` is only valid during its callback. aergo copies each
payload into a reused buffer before calling the handler. Elixir materializes the
payload as a BEAM binary, because a BEAM binary cannot alias mutable shared
memory. C, C++, Java, .NET and Rust echo the log buffer directly.

N concurrent pairs, aggregate round trips/s and the median pair's p50. Every
pinger finishes its warmup and then waits for one wall-clock start time that the
runner sets, so the timed loops of all N pairs run at the same time; the
aggregate is the total round trips of all pairs divided by the time from the
first pair's start to the last pair's finish. In this run every pinger began
within 14 ms of the start time.

| client | N=1 | N=4 | N=12 | p50 at N=1 | p50 at N=4 | p50 at N=12 |
|---|---|---|---|---|---|---|
| c | 6.80 M | 17.69 M | 14.49 M | 0.125 | 0.125 | 0.166 |
| cpp | 5.84 M | 16.17 M | 12.66 M | 0.125 | 0.166 | 0.167 |
| rust | 5.78 M | 16.36 M | 11.16 M | 0.125 | 0.125 | 0.208 |
| dotnet | 4.95 M | 14.46 M | 10.82 M | 0.166 | 0.167 | 0.209 |
| go | 3.97 M | 11.75 M | 7.67 M | 0.208 | 0.209 | 0.250 |
| java | 4.09 M | 8.60 M | 6.82 M | 0.167 | 0.292 | 0.167 |
| elixir | 2.89 M | 8.58 M | 5.80 M | 0.250 | 0.250 | 0.500 |
| python | 1.26 M | 4.19 M | 3.66 M | 0.708 | 0.833 | 1.083 |

Every client peaks at four pairs. Twelve pairs are 24 busy-spinning processes on
16 cores, so pairs take turns on a core and every aggregate falls. At twelve
pairs C carries the highest aggregate, 14.49 M round trips per second, followed
by C++ (12.66 M), Rust (11.16 M) and .NET (10.82 M); aeron_elixir carries
5.80 M, and its median pair's p50 doubles from 0.250 µs to 0.500 µs.

### Market data

`bench/harness/run.exs` runs a synthetic price publisher, a C program on the
Aeron C client, as its own OS process and has each client consume its 64-byte
ticks in turn, first with the publisher flat out, then at fixed rates. Each
cell is run three times and the median run is shown: the median consume rate
flat out, the median p99 at a fixed rate. Full method and commentary are in
`bench/results/harness.md`.

| client | published | consumed | consume rate/s | lag | gaps | p50 µs | p99 µs | p99.9 µs | max µs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| elixir_direct | 94.74M | 94.74M | 9.47M | 0.00M | 0 | >10000 | >10000 | >10000 | >10000 |
| python | 27.12M | 27.12M | 2.71M | 0.00M | 0 | >10000 | >10000 | >10000 | >10000 |
| java | 261.04M | 261.04M | 26.10M | 0.00M | 0 | >10000 | >10000 | >10000 | >10000 |
| c | 320.86M | 320.86M | 32.09M | 0.00M | 0 | 0.05 | 9.05 | 24.05 | 86.0 |
| go | 248.99M | 248.99M | 24.90M | 0.00M | 0 | >10000 | >10000 | >10000 | >10000 |
| dotnet | 278.52M | 278.52M | 27.85M | 0.00M | 0 | 0.05 | 5371.05 | >10000 | >10000 |
| rust | 334.02M | 334.02M | 33.40M | 0.00M | 0 | 9059.05 | 9470.05 | >10000 | >10000 |

Rate steps, p50 / p99 µs:

| client | 250k/s | 500k/s | 1000k/s | 2000k/s |
|---|---|---|---|---|
| elixir_direct | p50 0.05 p99 2.05 | p50 0.05 p99 2.05 | p50 0.05 p99 2.05 | p50 0.05 p99 3.05 |
| python | p50 0.05 p99 0.05 | p50 0.05 p99 0.05 | p50 0.05 p99 0.05 | p50 0.05 p99 4.05 |
| java | p50 0.55 p99 0.55 | p50 0.55 p99 0.55 | p50 0.55 p99 0.55 | p50 0.55 p99 0.55 |
| c | p50 0.05 p99 0.05 | p50 0.05 p99 0.05 | p50 0.05 p99 0.05 | p50 0.05 p99 0.05 |
| go | p50 0.05 p99 12.05 | p50 0.05 p99 12.05 | p50 0.05 p99 11.05 | p50 0.05 p99 11.05 |
| dotnet | p50 0.05 p99 0.05 | p50 0.05 p99 0.05 | p50 0.05 p99 0.05 | p50 0.05 p99 0.05 |
| rust | p50 0.05 p99 0.05 | p50 0.05 p99 0.05 | p50 0.05 p99 0.05 | p50 0.05 p99 0.05 |

Flat out, a consumer that back-pressures the publisher is reading as fast as it
can, so its consume rate is its own maximum. Rust reads 33.4 M ticks/s and C
32.1 M; C back-pressured the publisher for under a millisecond, so it kept pace
with the publisher itself. .NET reads 27.9 M, Java 26.1 M, Go 24.9 M,
aeron_elixir 9.5 M and Python 2.7 M. Saturation latencies are queue depth, not
transport cost.

Every side timestamps with CLOCK_REALTIME, which on macOS advances in 1 µs
steps, so the fixed-rate latencies are whole microseconds and a p50 of 0.05 µs
means the tick was read within the microsecond it was stamped. At that
resolution, C, .NET and Rust read 99% of ticks within the microsecond they were
stamped at every rate, aeron_elixir within 2–3 µs and Go within 11–12 µs; Python
matches the C-based clients up to 1 M/s and needs 4 µs at 2 M/s. `java` reports
0.55 µs where the others report 0.05 µs because its consumer adds half a clock
step to remove truncation bias.

## Web page

`docs/` holds the project's web page: the overview, the benchmark results with
charts for every suite and option, and a page per example. `elixir
site/build.exs` builds it from this checkout, reading `bench/results`,
`README.md` and `examples/`. Each page is a single HTML file with its styles and
scripts inline. GitHub Pages serves it from the `main` branch and the `/docs`
folder, so after a benchmark run, rebuild the page and commit it with the new
results. Example pages link to their source on GitHub when the `origin` remote
is a GitHub repository or `AERON_ELIXIR_REPO=owner/name` is set.

## Limits and roadmap

- `aeron:ipc` and `aeron:udp` both carry messages end to end, covered by
  `test/aeron_elixir/udp_test.exs` and `examples/12_udp.exs`. UDP is verified
  over loopback only; a second host would additionally exercise a 1500-byte MTU,
  packet loss and multicast. Spy subscriptions and response channels are not
  supported.
- No multi-destination cast or multi-destination subscription: the
  ADD_DESTINATION and REMOVE_DESTINATION commands are not sent.
- No `try_claim`; iodata publishing covers writing without an intermediate copy.
- No SBE codec generation. Payloads are opaque binaries.
- Driver observability is limited to single counters: no reader over all
  driver counters with their labels, no error log decoding, no loss report.
- Archive, Cluster, authentication and authorisation are not implemented.
- Planned: verify the UDP path, destinations for multi-destination cast,
  generate SBE codecs from a schema, a counters and error-log reader, and a relay
  client example for an Aeron Cluster egress stream.

## License

Apache License 2.0; see `LICENSE`. The Aeron sources the bundled driver is
built from are Apache 2.0; `NOTICE` credits them.
