Code.require_file("../shared.exs", __DIR__)

alias AeronElixir.Bench.Shared

# N independent producer/consumer pairs across cores. Each BEAM message carries
# `items_per_msg` binary-packed records; the consumer iterates every record, so
# the aggregate rate is real per-message throughput (not the zero-copy mirage).
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

defmodule BeamPair do
  @moduledoc false

  def run(parent, size, items, sync_batch, warmup_ns, time_ns) do
    record = :binary.copy(<<0x5A>>, size)
    msg = :binary.copy(record, items)
    consumer = spawn_link(fn -> BeamConsumer.loop(0, size) end)

    loop(consumer, msg, items, sync_batch, System.monotonic_time(:nanosecond) + warmup_ns, 0)

    start_ns = System.monotonic_time(:nanosecond)
    sent = loop(consumer, msg, items, sync_batch, start_ns + time_ns, 0)
    elapsed_ns = System.monotonic_time(:nanosecond) - start_ns

    send(consumer, :stop)
    send(parent, {:done, sent, elapsed_ns})
  end

  defp loop(consumer, msg, items, sync_batch, deadline, acc) do
    if System.monotonic_time(:nanosecond) >= deadline do
      acc
    else
      Enum.each(1..sync_batch, fn _ -> send(consumer, {:m, msg}) end)
      send(consumer, {:sync, self()})

      receive do
        :synced -> :ok
      end

      loop(consumer, msg, items, sync_batch, deadline, acc + sync_batch * items)
    end
  end
end

pairs = String.to_integer(System.get_env("BENCH_PAIRS", "#{div(System.schedulers_online(), 2)}"))
items_per_msg = String.to_integer(System.get_env("BENCH_ITEMS_PER_MSG", "100"))
sync_batch = String.to_integer(System.get_env("BENCH_MSGS_PER_SYNC", "100"))
warmup_ns = Shared.warmup_seconds() * 1_000_000_000
time_ns = Shared.time_seconds() * 1_000_000_000

for size <- Shared.payload_sizes() do
  parent = self()

  for _ <- 1..pairs do
    spawn_link(fn -> BeamPair.run(parent, size, items_per_msg, sync_batch, warmup_ns, time_ns) end)
  end

  results =
    for _ <- 1..pairs do
      receive do
        {:done, sent, elapsed_ns} -> {sent, elapsed_ns}
      end
    end

  total_sent = results |> Enum.map(&elem(&1, 0)) |> Enum.sum()
  max_elapsed = results |> Enum.map(&elem(&1, 1)) |> Enum.max()
  ops_per_sec = total_sent * 1_000_000_000.0 / max_elapsed

  IO.puts(
    "{\"client\":\"beam_#{pairs}pairs\",\"scenario\":\"throughput\"," <>
      "\"payload_size\":#{size},\"pairs\":#{pairs},\"items_per_msg\":#{items_per_msg}," <>
      "\"samples\":#{total_sent},\"ops_per_sec\":#{ops_per_sec},\"bytes_per_sec\":#{ops_per_sec * size}}"
  )
end
