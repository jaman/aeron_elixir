Code.require_file("../shared.exs", __DIR__)

alias AeronElixir.Bench.Shared

# v1 single-node OMS shape: per-instrument process owns the LP quote and fills;
# per-user connection process gates one order at a time, does the margin
# reserve/check, sends the order, awaits the fill, commits the position. A price
# feed pumps LP quotes concurrently. In-VM => orders are terms, no encode/decode.
# Out of scope (later layers): cross-node routing, per-user seq, durability.

defmodule Instrument do
  @moduledoc false

  def loop(bid, ask, quotes) do
    receive do
      {:quote, b, a} ->
        loop(b, a, quotes + 1)

      {:order, from, :buy, size} ->
        send(from, {:filled, ask, size})
        loop(bid, ask, quotes)

      {:order, from, :sell, size} ->
        send(from, {:filled, bid, size})
        loop(bid, ask, quotes)

      {:report, from} ->
        send(from, {:quotes, quotes})
        loop(bid, ask, quotes)

      :stop ->
        :ok
    end
  end
end

defmodule Trader do
  @moduledoc false
  @balance 1_000_000_000
  @rate 2
  @size 1000
  @nominal 10_000

  def run(parent, inst, warmup_ns, time_ns) do
    state = {0, 0, :buy}
    {state, _} = loop(parent, inst, state, System.monotonic_time(:nanosecond) + warmup_ns, 0)
    {_state, count} = loop(parent, inst, state, System.monotonic_time(:nanosecond) + time_ns, 0)
    send(parent, {:orders, count})
  end

  defp loop(parent, inst, {position, used, side} = state, deadline, count) do
    if System.monotonic_time(:nanosecond) >= deadline do
      {state, count}
    else
      required = @size * @nominal * @rate
      available = @balance - used

      if available >= required do
        send(inst, {:order, self(), side, @size})

        receive do
          {:filled, price, sz} ->
            signed = if side == :buy, do: sz, else: -sz
            new_position = position + signed
            new_used = abs(new_position) * price * @rate
            next_side = if side == :buy, do: :sell, else: :buy
            loop(parent, inst, {new_position, new_used, next_side}, deadline, count + 1)
        end
      else
        loop(parent, inst, {0, 0, :buy}, deadline, count)
      end
    end
  end
end

defmodule Feeder do
  @moduledoc false

  def run(instruments, deadline) do
    if System.monotonic_time(:nanosecond) >= deadline do
      :ok
    else
      Enum.each(instruments, fn inst -> send(inst, {:quote, 10_000, 10_001}) end)
      Process.sleep(1)
      run(instruments, deadline)
    end
  end
end

n_instruments = String.to_integer(System.get_env("BENCH_INSTRUMENTS", "50"))
n_users = String.to_integer(System.get_env("BENCH_USERS", "200"))
n_feeders = String.to_integer(System.get_env("BENCH_FEEDERS", "2"))
warmup_ns = Shared.warmup_seconds() * 1_000_000_000
time_ns = Shared.time_seconds() * 1_000_000_000

instruments =
  for _ <- 1..n_instruments, do: spawn_link(fn -> Instrument.loop(10_000, 10_001, 0) end)

parent = self()
total_deadline = System.monotonic_time(:nanosecond) + warmup_ns + time_ns

for _ <- 1..n_feeders do
  spawn_link(fn -> Feeder.run(instruments, total_deadline) end)
end

for i <- 1..n_users do
  inst = Enum.at(instruments, rem(i, n_instruments))
  spawn_link(fn -> Trader.run(parent, inst, warmup_ns, time_ns) end)
end

orders =
  for _ <- 1..n_users do
    receive do
      {:orders, count} -> count
    end
  end
  |> Enum.sum()

quotes =
  instruments
  |> Enum.map(fn inst ->
    send(inst, {:report, self()})

    receive do
      {:quotes, q} -> q
    end
  end)
  |> Enum.sum()

Enum.each(instruments, fn inst -> send(inst, :stop) end)

orders_per_sec = orders * 1_000_000_000.0 / time_ns
quotes_per_sec = quotes * 1_000_000_000.0 / (warmup_ns + time_ns)

IO.puts(
  "{\"scenario\":\"oms\",\"instruments\":#{n_instruments},\"users\":#{n_users}," <>
    "\"feeders\":#{n_feeders},\"orders\":#{orders}," <>
    "\"orders_per_sec\":#{Float.round(orders_per_sec, 1)}," <>
    "\"quotes_per_sec\":#{Float.round(quotes_per_sec, 1)}}"
)
