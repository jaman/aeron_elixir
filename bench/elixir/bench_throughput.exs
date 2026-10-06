require Logger

Code.require_file("../shared.exs", __DIR__)

alias AeronElixir.Bench.Shared

defmodule ThroughputRunner do
  def warmup(publish_batch, drain, payload, batch_size, deadline) do
    if System.monotonic_time(:nanosecond) >= deadline do
      :ok
    else
      publish_batch.(payload, batch_size)
      drain.()
      warmup(publish_batch, drain, payload, batch_size, deadline)
    end
  end

  def send_until(publish_batch, drain, payload, batch_size, deadline, acc) do
    if System.monotonic_time(:nanosecond) >= deadline do
      acc
    else
      publish_batch.(payload, batch_size)
      drain.()
      send_until(publish_batch, drain, payload, batch_size, deadline, acc + batch_size)
    end
  end
end

{:ok, _} = Application.ensure_all_started(:aeron_elixir)

client = Shared.start_client()

batch_size = 1_000

for size <- Shared.payload_sizes() do
  channel = Shared.unique_channel("throughput")
  stream_id = Shared.unique_stream_id()

  {pub, sub} = Shared.setup_pub_sub(client, channel, stream_id)
  payload = Shared.payload_pool(size)

  Logger.info("Warming up throughput bench: #{size} bytes x #{batch_size}")
  Shared.await_first_message(pub, sub, payload)

  publish_batch = Shared.make_publish_batch(pub, sub)
  drain = Shared.make_drain_all(sub)

  warmup_deadline = System.monotonic_time(:nanosecond) + Shared.warmup_seconds() * 1_000_000_000
  ThroughputRunner.warmup(publish_batch, drain, payload, batch_size, warmup_deadline)

  start_ns = System.monotonic_time(:nanosecond)
  run_deadline = start_ns + Shared.time_seconds() * 1_000_000_000
  sent = ThroughputRunner.send_until(publish_batch, drain, payload, batch_size, run_deadline, 0)
  elapsed_ns = System.monotonic_time(:nanosecond) - start_ns
  drain.()
  IO.puts(:stderr, "[bench-elixir] throughput tick_sum=#{Shared.tick_sum_total()}")

  ops_per_sec = sent * 1_000_000_000.0 / elapsed_ns
  bytes_per_sec = ops_per_sec * size

  IO.puts(
    "{\"client\":\"#{Shared.client_label()}\",\"scenario\":\"throughput\"," <>
      "\"payload_size\":#{size}," <>
      "\"samples\":#{sent}," <>
      "\"ops_per_sec\":#{ops_per_sec}," <>
      "\"bytes_per_sec\":#{bytes_per_sec}," <>
      "\"elapsed_ms\":#{elapsed_ns / 1_000_000.0}}"
  )

  AeronElixir.close(pub)
  AeronElixir.close(sub)
end

AeronElixir.close(AeronElixir.client(client))
