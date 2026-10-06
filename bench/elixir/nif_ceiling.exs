defmodule NifCeiling.C do
  @moduledoc """
  `add/2` written against erl_nif with no wrapper, built from
  `bench/elixir/nif_ceiling/add_nif.c`.
  """

  def load(path), do: :erlang.load_nif(String.to_charlist(path), 0)
  def add(_a, _b), do: :erlang.nif_error(:not_loaded)
end

defmodule NifCeiling do
  @moduledoc """
  Measures whether the cost of a NIF call rises with the number of processes
  calling NIFs at once.

      AERON_DIR=/tmp/ae_drv mix run bench/elixir/nif_ceiling.exs
      PROBE_CALLERS=1,4,8,12 PROBE_CALLS=5000000 mix run bench/elixir/nif_ceiling.exs

  Every caller runs the same loop for `PROBE_CALLS` calls after a common start.
  ns/call is the median over callers of each caller's elapsed time divided by its
  calls; M calls/s is all calls over wall time.

  Rows: `bif` is a pure BIF; `c_add` is a trivial C NIF; `aeron_read` is
  aeron_elixir's own `read_int64/1`, which needs a running driver at `AERON_DIR`
  and is skipped without one; `crypto` is OTP's `:crypto.hash/2`. The control
  runs one `c_add` caller among N-1 busy processes that call no NIFs.
  """

  alias AeronElixir.NIF

  def loop(:bif, n, _arg), do: bif(n, 0)
  def loop(:c_add, n, _arg), do: c_add(n, 0)
  def loop(:aeron_read, n, address), do: aeron_read(n, address)
  def loop(:crypto, n, _arg), do: crypto(n)
  def loop(:spin, n, _arg), do: spin(n, 0)

  def run(kind, callers, calls, arg, spinners \\ 0) do
    parent = self()
    spin_pids = for _ <- 1..spinners//1, do: spawn_link(fn -> spin_until_stopped(parent) end)
    pids = for _ <- 1..callers, do: spawn_link(fn -> timed_loop(parent, kind, calls, arg) end)
    Enum.each(spin_pids ++ pids, &send(&1, :go))

    times = for _ <- 1..callers, do: receive(do: ({:done, t0, t1} -> {t0, t1}))
    Enum.each(spin_pids, &send(&1, :stop))
    for _ <- spin_pids, do: receive(do: (:spun -> :ok))

    per_call = times |> Enum.map(fn {t0, t1} -> (t1 - t0) / calls end) |> median()
    wall = Enum.max(Enum.map(times, &elem(&1, 1))) - Enum.min(Enum.map(times, &elem(&1, 0)))
    {per_call, callers * calls * 1_000.0 / wall}
  end

  defp timed_loop(parent, kind, calls, arg) do
    receive do
      :go -> :ok
    end

    t0 = System.monotonic_time(:nanosecond)
    loop(kind, calls, arg)
    send(parent, {:done, t0, System.monotonic_time(:nanosecond)})
  end

  defp bif(0, acc), do: acc
  defp bif(n, acc), do: bif(n - 1, acc + :erlang.phash2(n))

  defp c_add(0, acc), do: acc
  defp c_add(n, acc), do: c_add(n - 1, NifCeiling.C.add(acc, 1))

  defp aeron_read(0, _address), do: :ok

  defp aeron_read(n, address) do
    {:ok, _} = NIF.read_int64(address)
    aeron_read(n - 1, address)
  end

  defp crypto(0), do: :ok

  defp crypto(n) do
    :crypto.hash(:sha256, "x")
    crypto(n - 1)
  end

  defp spin(0, acc), do: acc
  defp spin(n, acc), do: spin(n - 1, rem(acc * 31 + n, 1_000_003))

  defp spin_until_stopped(parent) do
    receive do
      :go -> :ok
    end

    spin_chunk(parent)
  end

  defp spin_chunk(parent) do
    spin(100_000, 0)

    receive do
      :stop -> send(parent, :spun)
    after
      0 -> spin_chunk(parent)
    end
  end

  defp median(list), do: list |> Enum.sort() |> Enum.at(div(length(list), 2))
end

int_list = fn name, default ->
  name |> System.get_env(default) |> String.split(",") |> Enum.map(&String.to_integer/1)
end

callers = int_list.("PROBE_CALLERS", "1,2,4,8,12,16,20")
calls = String.to_integer(System.get_env("PROBE_CALLS", "5000000"))
aeron_dir = System.get_env("AERON_DIR", "/tmp/ae_drv")
pid_field_offset = 40

{cnc_region, aeron_address} =
  case AeronElixir.NIF.map_cnc(Path.join(aeron_dir, "cnc.dat")) do
    {:ok, region, base} -> {region, base + pid_field_offset}
    {:error, _reason} -> {nil, nil}
  end

c_source = Path.expand("nif_ceiling/add_nif.c", __DIR__)
c_library = Path.join(Mix.Project.build_path(), "nif_ceiling/add_nif.so")
File.mkdir_p!(Path.dirname(c_library))
erts_include = Path.join([:code.root_dir(), "erts-#{:erlang.system_info(:version)}", "include"])

shared_flags =
  if match?({:unix, :darwin}, :os.type()),
    do: ["-bundle", "-undefined", "dynamic_lookup"],
    else: ["-shared"]

{_, 0} = System.cmd("cc", ["-O3", "-fPIC", "-I", erts_include] ++ shared_flags ++ ["-o", c_library, c_source])
:ok = NifCeiling.C.load(Path.rootname(c_library))

kinds = [:bif, :c_add] ++ if(aeron_address, do: [:aeron_read], else: []) ++ [:crypto]

IO.puts(
  "otp=#{System.otp_release()} schedulers=#{System.schedulers_online()} " <>
    "calls/caller=#{calls} aeron_read=#{if aeron_address, do: aeron_dir, else: "skipped"}\n"
)

format = fn value, width -> value |> :erlang.float_to_binary(decimals: 1) |> String.pad_leading(width) end

IO.puts(
  "callers " <>
    Enum.map_join(kinds, "", &String.pad_leading("#{&1} ns", 16)) <>
    "   " <> Enum.map_join(kinds, "", &String.pad_leading("#{&1} M/s", 16))
)

for n <- callers do
  results =
    for kind <- kinds do
      NifCeiling.run(kind, n, div(calls, 10), aeron_address)
      NifCeiling.run(kind, n, calls, aeron_address)
    end

  IO.puts(
    String.pad_trailing("#{n}", 8) <>
      Enum.map_join(results, "", fn {ns, _} -> format.(ns, 16) end) <>
      "   " <> Enum.map_join(results, "", fn {_, rate} -> format.(rate, 16) end)
  )
end

IO.puts("\ncontrol: 1 c_add caller + (N-1) busy processes that call no NIFs")

for n <- callers do
  NifCeiling.run(:c_add, 1, div(calls, 10), nil, n - 1)
  {ns, _} = NifCeiling.run(:c_add, 1, calls, nil, n - 1)
  IO.puts("  N=#{String.pad_trailing("#{n}", 4)} #{format.(ns, 8)} ns/call")
end

IO.puts("cnc mapping held: #{is_reference(cnc_region) or is_nil(cnc_region)}")
