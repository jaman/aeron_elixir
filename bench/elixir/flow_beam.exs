Code.require_file("../shared.exs", __DIR__)

alias AeronElixir.Bench.Shared

# Realistic data flow: each tick is a FRESH binary built per message (encode cost),
# and the consumer DECODES every message (read 4 fields) and accumulates (process
# cost). Mirrors the native benches' price-tick work for a fair comparison.
# Layout (little-endian): instrument_id u32 @0, bid i64 @4, ask i64 @12, seq i64 @20.

defmodule FlowConsumer do
  @moduledoc false

  def loop(count, sum) do
    receive do
      {:t, msg} ->
        <<inst::little-unsigned-32, bid::little-signed-64, ask::little-signed-64,
          seq::little-signed-64, _rest::binary>> = msg

        loop(count + 1, sum + bid + inst + ask + seq)

      {:sync, from} ->
        send(from, :synced)
        loop(count, sum)

      {:report, from} ->
        send(from, {:creport, count, sum})
        loop(count, sum)
    end
  end
end

defmodule FlowPair do
  @moduledoc false

  def run(parent, size, sync_batch, warmup_ns, time_ns) do
    pad = :binary.copy(<<0>>, max(size - 28, 0))
    consumer = spawn_link(fn -> FlowConsumer.loop(0, 0) end)

    seq1 = loop(consumer, pad, 0, sync_batch, System.monotonic_time(:nanosecond) + warmup_ns, 0)
    {seq1, _} = seq1

    start_ns = System.monotonic_time(:nanosecond)
    {_seq, sent} = loop(consumer, pad, seq1, sync_batch, start_ns + time_ns, 0)
    elapsed_ns = System.monotonic_time(:nanosecond) - start_ns

    send(consumer, {:report, self()})
    csum = receive do
      {:creport, _ccount, sum} -> sum
    end

    send(parent, {:done, sent, elapsed_ns, csum})
  end

  defp loop(consumer, pad, seq, sync_batch, deadline, acc) do
    if System.monotonic_time(:nanosecond) >= deadline do
      {seq, acc}
    else
      next_seq = send_batch(consumer, pad, seq, sync_batch)
      loop(consumer, pad, next_seq, sync_batch, deadline, acc + sync_batch)
    end
  end

  defp send_batch(consumer, pad, seq, 0) do
    send(consumer, {:sync, self()})

    receive do
      :synced -> :ok
    end

    seq
  end

  defp send_batch(consumer, pad, seq, n) do
    inst = rem(seq, 100)
    bid = 100_000 + rem(seq, 1000)
    ask = bid + 10

    msg =
      <<inst::little-unsigned-32, bid::little-signed-64, ask::little-signed-64,
        seq::little-signed-64>> <> pad

    send(consumer, {:t, msg})
    send_batch(consumer, pad, seq + 1, n - 1)
  end
end

pairs = String.to_integer(System.get_env("BENCH_PAIRS", "#{div(System.schedulers_online(), 2)}"))
sync_batch = String.to_integer(System.get_env("BENCH_MSGS_PER_SYNC", "100"))
warmup_ns = Shared.warmup_seconds() * 1_000_000_000
time_ns = Shared.time_seconds() * 1_000_000_000

for size <- Shared.payload_sizes() do
  parent = self()

  for _ <- 1..pairs do
    spawn_link(fn -> FlowPair.run(parent, size, sync_batch, warmup_ns, time_ns) end)
  end

  results =
    for _ <- 1..pairs do
      receive do
        {:done, sent, elapsed_ns, csum} -> {sent, elapsed_ns, csum}
      end
    end

  total_sent = results |> Enum.map(&elem(&1, 0)) |> Enum.sum()
  max_elapsed = results |> Enum.map(&elem(&1, 1)) |> Enum.max()
  checksum = results |> Enum.map(&elem(&1, 2)) |> Enum.reduce(0, &Bitwise.bxor/2)
  ops_per_sec = total_sent * 1_000_000_000.0 / max_elapsed

  IO.puts(
    "{\"client\":\"beam_flow_#{pairs}p\",\"scenario\":\"throughput\"," <>
      "\"payload_size\":#{size},\"pairs\":#{pairs}," <>
      "\"samples\":#{total_sent},\"ops_per_sec\":#{ops_per_sec}," <>
      "\"bytes_per_sec\":#{ops_per_sec * size},\"checksum\":#{checksum}}"
  )
end
