Code.require_file("../shared.exs", __DIR__)

alias AeronElixir.Bench.Shared

defmodule BeamMultiConsumer do
  @moduledoc false

  def loop(count, sum) do
    receive do
      {:m, bin} -> loop(count + 1, Shared.tick_sum(bin, sum))
      {:sync, from} -> send(from, {:synced, count}); loop(count, sum)
      {:stop, from} -> send(from, {:stopped, count, sum})
    end
  end
end

defmodule BeamMultiPair do
  @moduledoc false

  @batch 1_000

  def run(parent, size, warmup_ns, time_ns) do
    payload = Shared.payload_pool(size)
    consumer = spawn_link(fn -> BeamMultiConsumer.loop(0, 0) end)

    loop(consumer, payload, System.monotonic_time(:nanosecond) + warmup_ns, 0)
    send(consumer, {:stop, self()})

    receive do
      {:stopped, _count, _sum} -> :ok
    end

    consumer = spawn_link(fn -> BeamMultiConsumer.loop(0, 0) end)
    start_ns = System.monotonic_time(:nanosecond)
    sent = loop(consumer, payload, start_ns + time_ns, 0)
    elapsed_ns = System.monotonic_time(:nanosecond) - start_ns

    send(consumer, {:stop, self()})

    receive do
      {:stopped, received, sum} -> send(parent, {:done, sent, received, elapsed_ns, sum})
    end
  end

  defp loop(consumer, payload, deadline, sent) do
    if System.monotonic_time(:nanosecond) >= deadline do
      sent
    else
      send_batch(consumer, payload, sent, @batch)
      send(consumer, {:sync, self()})

      receive do
        {:synced, _count} -> :ok
      end

      loop(consumer, payload, deadline, sent + @batch)
    end
  end

  defp send_batch(_consumer, _payload, _sequence, 0), do: :ok

  defp send_batch(consumer, payload, sequence, remaining) do
    send(consumer, {:m, IO.iodata_to_binary(Shared.message(payload, sequence))})
    send_batch(consumer, payload, sequence + 1, remaining - 1)
  end
end

pairs = String.to_integer(System.get_env("BENCH_PAIRS", "4"))
warmup_ns = Shared.warmup_seconds() * 1_000_000_000
time_ns = Shared.time_seconds() * 1_000_000_000

for size <- Shared.payload_sizes() do
  parent = self()

  for _ <- 1..pairs do
    spawn_link(fn -> BeamMultiPair.run(parent, size, warmup_ns, time_ns) end)
  end

  results =
    for _ <- 1..pairs do
      receive do
        {:done, sent, received, elapsed_ns, sum} -> {sent, received, elapsed_ns, sum}
      end
    end

  total_sent = results |> Enum.map(&elem(&1, 0)) |> Enum.sum()
  total_received = results |> Enum.map(&elem(&1, 1)) |> Enum.sum()
  max_elapsed = results |> Enum.map(&elem(&1, 2)) |> Enum.max()
  tick_sum = results |> Enum.map(&elem(&1, 3)) |> Enum.sum()
  ops = total_sent * 1_000_000_000.0 / max_elapsed

  IO.puts(:stderr, "[bench-beam-mp] tick_sum=#{tick_sum}")

  IO.puts(
    "{\"client\":\"beam\",\"scenario\":\"throughput\"," <>
      "\"payload_size\":#{size},\"workers\":#{pairs}," <>
      "\"samples\":#{total_sent},\"received\":#{total_received}," <>
      "\"ops_per_sec\":#{ops},\"bytes_per_sec\":#{ops * size}}"
  )
end
