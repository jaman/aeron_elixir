# AeronElixir Agent Instructions

## Build and Native Code

- `mix compile` builds the native code with the system C compiler (`$CC`, default `cc`) through `Mix.Tasks.Compile.AeronNative`; no Makefile
- The NIF source is `c_src/aeron_elixir_nif.c`, built `-O3` into `priv/lib/aeron_elixir_nif.so` and loaded by `AeronElixir.NIF`
- The bundled Aeron C media driver is built into `priv/bin/aeronmd` from the Aeron 1.51.0 C client and driver sources plus `c_src/aeron_driver/owner_watch.c`
- `Mix.Tasks.Compile.AeronNative.AeronSources` downloads those sources on the first build (pinned SHA-512) into `_build/aeron-1.51.0`; `AERON_SOURCE_DIR` points it at a local unpacked 1.51.0 tree instead
- Each output rebuilds only when one of its sources is newer than it
- `mix aeron.driver` prints the driver mode, directory and binary in use

## Developer Commands

- `mix compile` - Compiles Elixir, the C NIF and the media driver
- `mix test` - Runs all tests
- `mix quality` - Runs format + credo --strict + dialyzer
- `mix quality.ci` - Same but checks format without modifying
- `mix format` - Formats code
- `elixir site/build.exs` - Rebuilds the static web page in `docs/` from `bench/results`, `README.md` and `examples/`

## Testing

- Test tags: `:system`, `:interop`, `:protocol`
- System tests use the application's driver: the bundled driver it starts, or the external driver `AERON_DIR` names; no separate driver is needed
- Driver loss is covered by `test/aeron_elixir/driver_loss_test.exs`
- Interoperability tests use `async: false`
- Run specific test: `mix test test/aeron_elixir/system_test.exs`

## Ash Architecture

- Domain: `AeronElixir.Domain` wires all resources
- Resources (in `lib/aeron_elixir/resources/`): Client, Publication, Subscription, Image, Counter
- Data layer: Ash.DataLayer.Ets
- Code interfaces provide generated functions like `Client.connect/1`
- All resources use Ash actions - bypass in favor of direct protocol code only in native layer

## Quality

- Max line length: 120 characters
- Credo runs in strict mode
- Dialyzer PLT at `priv/plts/dialyzer.plt`
- No comments in code; documentation lives in `@moduledoc` and `@doc` only

## Key Modules

- `AeronElixir` - Public API (client lifecycle, publish, poll, images, handles)
- `AeronElixir.ClientConductor` - Driver handshake, keepalive, image discovery (control plane)
- `AeronElixir.RuntimeHandles` - Per-client ETS table of publication and image handles read by the hot path
- `AeronElixir.LogBuffer.Publisher` / `Subscriber` - Runtime handles over mapped log buffers
- `AeronElixir.BatchPublisher` - Mailbox-draining publisher process
- `AeronElixir.NIF` - C NIF: mmap, atomics, term append and read
- `AeronElixir.CnC.Reader` - CnC file operations
- `AeronElixir.Protocol.*` - Protocol codecs (Control, Frame, URI)
- `AeronElixir.ChannelUri`, `AeronElixir.Idle` - URI builders, idle strategies
- `AeronElixir.MediaDriver` - runs a driver binary (bundled by default) as a Port; the application supervises one as `AeronElixir.EmbeddedDriver`
- `AeronElixir.DriverConfig` - resolves embedded vs external driver, its directory (`AERON_DIR` > `config :aeron_elixir, :aeron_dir` > private runtime dir) and binary
- `AeronElixir.Resources.*` - Ash resources for domain objects
