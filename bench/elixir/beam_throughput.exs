Code.require_file("../shared.exs", __DIR__)

alias AeronElixir.Bench.Shared

# Each BEAM message carries `items_per_msg` fixed-size records, binary-packed.
# The consumer iterates every record (splits each sub-binary) so the reported
# rate is real per-message delivery+access throughput, never the zero-copy
# "count via byte_size" mirage. Set BENCH_ITEMS_PER_MSG=1 for the one-message-
# per-item floor.
defmodule BeamConsumer do
  @moduledoc false

  def loop(count, rs) do
    receive do
      {:m, bin} -> loop(count + touch(bin, rs, 0), rs)
      {:sync, from} -> send(from, :synced); loop(count, rs)
      :stop -> :ok
    end
  end

  defp touch(bin, rs, acc) do
    case bin do
      <<>> -> acc
      <<_rec::binary-size(rs), rest::binary>> -> touch(rest, rs, acc + 1)
    end
  end
end

defmodule BeamProducer do
  @moduledoc false

  def run(consumer, pool, items, sync_batch, deadline, acc) do
    if System.monotonic_time(:nanosecond) >= deadline do
      acc
    else
      send_batch(consumer, pool, items, sync_batch, acc)
      run(consumer, pool, items, sync_batch, deadline, acc + sync_batch * items)
    end
  end

  defp send_batch(consumer, pool, items, sync_batch, first_sequence) do
    Enum.each(0..(sync_batch - 1), fn index ->
      send(consumer, {:m, records(pool, first_sequence + index * items, items)})
    end)
    send(consumer, {:sync, self()})

    receive do
      :synced -> :ok
    end
  end

  defp records(pool, first_sequence, items),
    do: IO.iodata_to_binary(for(offset <- 0..(items - 1), do: Shared.message(pool, first_sequence + offset)))
end

items_per_msg = String.to_integer(System.get_env("BENCH_ITEMS_PER_MSG", "100"))
sync_batch = String.to_integer(System.get_env("BENCH_MSGS_PER_SYNC", "100"))

for size <- Shared.payload_sizes() do
  pool = Shared.payload_pool(size)
  consumer = spawn_link(fn -> BeamConsumer.loop(0, size) end)

  warmup_deadline = System.monotonic_time(:nanosecond) + Shared.warmup_seconds() * 1_000_000_000
  BeamProducer.run(consumer, pool, items_per_msg, sync_batch, warmup_deadline, 0)

  start_ns = System.monotonic_time(:nanosecond)
  run_deadline = start_ns + Shared.time_seconds() * 1_000_000_000
  sent = BeamProducer.run(consumer, pool, items_per_msg, sync_batch, run_deadline, 0)
  elapsed_ns = System.monotonic_time(:nanosecond) - start_ns

  send(consumer, :stop)

  ops_per_sec = sent * 1_000_000_000.0 / elapsed_ns

  IO.puts(
    "{\"client\":\"beam\",\"scenario\":\"throughput\"," <>
      "\"payload_size\":#{size}," <>
      "\"items_per_msg\":#{items_per_msg}," <>
      "\"samples\":#{sent}," <>
      "\"ops_per_sec\":#{ops_per_sec}," <>
      "\"bytes_per_sec\":#{ops_per_sec * size}}"
  )
end
