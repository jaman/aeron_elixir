defmodule Probe2 do
  # consumer counts LOGICAL items (list length) until it has seen `target`
  def consume(count, target, parent) do
    if count >= target do
      send(parent, :received)
    else
      receive do
        {:batch, list} -> consume(count + length(list), target, parent)
      end
    end
  end

  def fire(_c, _msg, 0), do: :ok
  def fire(c, msg, sends) do
    send(c, {:batch, msg})
    fire(c, msg, sends - 1)
  end
end

n_logical = String.to_integer(System.get_env("PROBE_N", "20000000"))
size = String.to_integer(System.get_env("PROBE_SIZE", "256"))
payload = :binary.copy(<<0x5A>>, size)
mono = fn -> System.monotonic_time(:nanosecond) end

run = fn batch ->
  msg = List.duplicate(payload, batch)
  sends = div(n_logical, batch)
  parent = self()
  c = spawn(fn -> Probe2.consume(0, n_logical, parent) end)
  t0 = mono.()
  Probe2.fire(c, msg, sends)
  receive do
    :received -> :ok
  end
  mono.() - t0
end

report = fn batch ->
  run.(batch)
  e = run.(batch)
  rate = Float.round(n_logical * 1_000.0 / e, 1)
  IO.puts("items/msg=#{String.pad_trailing("#{batch}", 6)} -> #{rate} M logical msg/s")
end

IO.puts("n_logical=#{n_logical} size=#{size}B\n")
for batch <- [1, 10, 100, 1000, 10000], do: report.(batch)
