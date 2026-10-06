# Examples

Each script uses the driver the application selects: the bundled driver it
starts, or the driver `AERON_DIR` names. No setup is needed:

```sh
mix run examples/01_ipc_producer_consumer.exs
```

`10_lifecycle.exs` also starts and stops a second driver of its own with
`AeronElixir.MediaDriver`.

## Mapping to the pyaeron examples

| pyaeron example | script | aeron_elixir API used |
|---|---|---|
| 01 producer / consumer | `01_ipc_producer_consumer.exs` | `add_publication/3`, `add_subscription/3`, `await_connected/2`, `publish/2`, `poll_batch/2` |
| 02 IPC fan-out | `02_ipc_fan_out.exs` | two subscriptions on one channel and stream, `publish_list/2` |
| 03 RPC | `03_rpc.exs` | request and response streams, correlation id carried in the payload as iodata |
| 04 offer status | `04_offer_status.exs` | `try_publish/2` → `:not_connected`, `:back_pressured`, `:closed`; `position/1`, `position_limit/1` |
| 05 polling | `05_polling.exs` | `poll/3` with a fragment limit, `AeronElixir.Idle` duty cycle, reassembled fragments, `header.position` |
| 06 channels | `06_channels.exs` | `AeronElixir.ChannelUri.ipc/1`, `udp/1`, validation errors, `Protocol.URI.parse/1` |
| 07 streams | `07_streams.exs` | independent stream ids on one channel |
| 08 exclusive publication | `08_exclusive_publication.exs` | `add_exclusive_publication/3`, distinct sessions, `image_count/1` |
| 09 buffer protocol | `09_iodata.exs` | binaries and nested iodata published without flattening |
| 10 lifecycle | `10_lifecycle.exs` | `AeronElixir.MediaDriver`, `connected?/1`, `close/1`, `closed?/1` |
| 11 images | `11_images.exs` | `available_image` / `unavailable_image` callbacks, `image_by_session_id/2`, `poll_image/3`, `poll_image_batch/2`, `image_position/1`, `end_of_stream?/1` |

## pyaeron API → aeron_elixir

| pyaeron | aeron_elixir |
|---|---|
| `Aeron(dir=…)` | `AeronElixir.start_link(aeron_directory: …)` |
| `Aeron(embedded=True)` | the default: the application starts the bundled driver and supervises it; `AeronElixir.MediaDriver.start_link(aeron_dir: …)` starts more |
| `Aeron(dir=…)` with an external driver | `AERON_DIR` or `config :aeron_elixir, aeron_dir: …` |
| `DriverTimeout` | the client's process exits with `{:shutdown, :driver_timeout}` and its publications and subscriptions are closed |
| `add_publication`, `add_exclusive_publication` | `add_publication/3`, `add_exclusive_publication/3` |
| `add_subscription(available_image=, unavailable_image=)` | `add_subscription/4` with `available_image:` / `unavailable_image:` |
| `offer` → `True` / `BACK_PRESSURED` / `NOT_CONNECTED` / `ADMIN_ACTION` | `try_publish/2` → `{:ok, position}` / `{:error, :back_pressured \| :not_connected \| :admin_action}` |
| `PublicationClosed`, `MaxPositionExceeded` | `{:error, :closed}`, `{:error, :max_position_exceeded}` |
| `offerv`, buffer-protocol payloads | iodata accepted by `publish/2`, `publish_list/2` |
| `await_connected`, `is_connected` | `await_connected/2`, `connected?/1` |
| `position`, `position_limit` | `position/1`, `position_limit/1` |
| `session_id`, `stream_id`, `channel`, `max_message_length`, `max_payload_length`, `term_buffer_length` | fields of the `Publication` record |
| `Subscription.poll(handler, fragment_limit)` | `poll/3` (handler gets `payload, header`; read `header` with `AeronElixir.Header`) |
| `ACTION_CONTINUE / COMMIT / BREAK / ABORT` | `controlled_poll/3` with handler returning `:continue \| :commit \| :break \| :abort` |
| `image_count`, `image_at_index`, `image_by_session_id` | `image_count/1`, `images/1`, `image_by_session_id/2` |
| `Image.poll`, `position`, `session_id`, `source_identity`, `is_end_of_stream` | `poll_image/3`, `poll_image_batch/2`, `image_position/1`, `image.session_id`, `image.source_identity`, `end_of_stream?/1` |
| `Header` fields | `%Protocol.Frame{}` incl. `initial_term_id` and `position` |
| `IdleStrategy` | `AeronElixir.Idle` (`:busy_spin`, `:yield`, `{:sleep, ms}`, `backoff/1`) |
| `close`, `is_closed` | `close/1`, `closed?/1` |

Not provided: `try_claim` / `BufferClaim` (needs a mutable buffer the caller
writes into; iodata `publish` covers writing without an intermediate copy) and
`Image.is_closed` (an image that has left the subscription is no longer listed by
`images/1`).
