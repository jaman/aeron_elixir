defmodule Probe3 do
  # one BEAM message carries a binary of N fixed-size records
  def consume_fast(count, target, parent, rs) do
    if count >= target do
      send(parent, :received)
    else
      receive do
        {:b, bin} -> consume_fast(count + div(byte_size(bin), rs), target, parent, rs)
      end
    end
  end

  def consume_touch(count, target, parent, rs) do
    if count >= target do
      send(parent, :received)
    else
      receive do
        {:b, bin} -> consume_touch(count + touch(bin, rs, 0), target, parent, rs)
      end
    end
  end

  defp touch(<<>>, _rs, acc), do: acc
  defp touch(bin, rs, acc) do
    <<rec::binary-size(rs), rest::binary>> = bin
    # touch the record's bytes so it's not optimized away
    touch(rest, rs, acc + :binary.first(rec))
  end

  def fire(_c, _msg, 0), do: :ok
  def fire(c, msg, sends) do
    send(c, {:b, msg})
    fire(c, msg, sends - 1)
  end
end

n_logical = String.to_integer(System.get_env("PROBE_N", "20000000"))
rs = String.to_integer(System.get_env("PROBE_SIZE", "256"))
record = :binary.copy(<<1>>, rs)
mono = fn -> System.monotonic_time(:nanosecond) end

run = fn n_per_msg, consume_fun ->
  msg = :binary.copy(record, n_per_msg)
  sends = div(n_logical, n_per_msg)
  parent = self()
  c = spawn(fn -> consume_fun.(0, n_logical, parent, rs) end)
  t0 = mono.()
  Probe3.fire(c, msg, sends)
  receive do
    :received -> :ok
  end
  mono.() - t0
end

report = fn n_per_msg ->
  run.(n_per_msg, &Probe3.consume_fast/4)
  ef = run.(n_per_msg, &Probe3.consume_fast/4)
  run.(n_per_msg, &Probe3.consume_touch/4)
  et = run.(n_per_msg, &Probe3.consume_touch/4)
  fast = Float.round(n_logical * 1_000.0 / ef, 1)
  touch = Float.round(n_logical * 1_000.0 / et, 1)
  IO.puts("records/msg=#{String.pad_trailing("#{n_per_msg}", 7)} transport=#{fast} M/s   touch-each=#{touch} M/s")
end

IO.puts("n_logical=#{n_logical} record=#{rs}B (binary-packed)\n")
for n_per_msg <- [1, 100, 1000, 10000, 65536], do: report.(n_per_msg)
