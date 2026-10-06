defmodule Probe do
  # pipelined consumer: count to target, then signal parent
  def consume(count, target, parent) do
    if count >= target do
      send(parent, :received)
    else
      receive do
        _ -> consume(count + 1, target, parent)
      end
    end
  end

  def fire_int(_c, 0), do: :ok
  def fire_int(c, n) do
    send(c, 1)
    fire_int(c, n - 1)
  end

  def fire_bin(_c, _p, 0), do: :ok
  def fire_bin(c, p, n) do
    send(c, {:m, p})
    fire_bin(c, p, n - 1)
  end

  # barrier consumer
  def bconsume(count) do
    receive do
      {:m, _} -> bconsume(count + 1)
      {:sync, from} -> send(from, :synced); bconsume(count)
      :stop -> :ok
    end
  end

  def fire_barrier(_c, _p, 0, _batch), do: :ok
  def fire_barrier(c, p, remaining, batch) do
    k = min(remaining, batch)
    Enum.each(1..k, fn _ -> send(c, {:m, p}) end)
    send(c, {:sync, self()})
    receive do
      :synced -> :ok
    end
    fire_barrier(c, p, remaining - k, batch)
  end
end

n = String.to_integer(System.get_env("PROBE_N", "2000000"))
size = String.to_integer(System.get_env("PROBE_SIZE", "256"))
payload = :binary.copy(<<0x5A>>, size)
mono = fn -> System.monotonic_time(:nanosecond) end
mrate = fn elapsed -> Float.round(n * 1_000.0 / elapsed, 2) end

run_pipe = fn fire_fn ->
  parent = self()
  c = spawn(fn -> Probe.consume(0, n, parent) end)
  t0 = mono.()
  fire_fn.(c)
  receive do
    :received -> :ok
  end
  mono.() - t0
end

run_barrier = fn batch ->
  c = spawn(fn -> Probe.bconsume(0) end)
  t0 = mono.()
  Probe.fire_barrier(c, payload, n, batch)
  send(c, :stop)
  mono.() - t0
end

# warm each once, report second run
report = fn label, fun ->
  fun.()
  e = fun.()
  IO.puts("#{String.pad_trailing(label, 34)} #{mrate.(e)} M msg/s  (#{Float.round(e / 1_000_000, 1)} ms)")
end

IO.puts("n=#{n} size=#{size}B  schedulers=#{System.schedulers_online()}\n")
report.("1. integer, pipelined (ceiling)", fn -> run_pipe.(fn c -> Probe.fire_int(c, n) end) end)
report.("2. {:m, bin} pipelined", fn -> run_pipe.(fn c -> Probe.fire_bin(c, payload, n) end) end)
report.("3. {:m, bin} barrier batch=1000", fn -> run_barrier.(1000) end)
report.("4. {:m, bin} barrier batch=100", fn -> run_barrier.(100) end)
report.("5. {:m, bin} barrier batch=10000", fn -> run_barrier.(10000) end)
