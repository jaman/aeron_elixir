defmodule AeronElixir.Bench.RpcRunner do
  @moduledoc false

  @clients ~w(c cpp java python go dotnet rust elixir)a
  @default_pairs [1, 4, 12]
  @message_length 32
  @warmup_messages 100_000
  @messages 1_000_000
  @cell_timeout_ms 300_000
  @stream_base 7_100
  @stream_span 100
  @start_margin_ns 10_000_000_000

  def run(opts \\ []) do
    pairs_list = Keyword.get(opts, :pairs, @default_pairs)
    clients = Keyword.get(opts, :clients, @clients)
    messages = Keyword.get(opts, :messages, @messages)
    warmup = Keyword.get(opts, :warmup, @warmup_messages)

    results_dir = Path.join(File.cwd!(), "bench/results")
    File.mkdir_p!(results_dir)

    cells = for client <- clients, pairs <- pairs_list, do: {client, pairs}
    total = length(cells)

    results =
      cells
      |> Enum.with_index()
      |> Enum.map(fn {{client, pairs}, idx} ->
        IO.puts(
          "\n[#{idx + 1}/#{total}] rpc #{client} pairs=#{pairs} (warmup=#{warmup} messages=#{messages})"
        )

        base = @stream_base + idx * @stream_span
        cell = run_cell(client, pairs, base, warmup, messages)
        IO.puts(summary_line(cell))
        cell
      end)

    merged = merge_existing(results, results_dir, Keyword.get(opts, :merge, false))
    write_json(merged, results_dir)
    markdown = render_markdown(merged, pairs_list, warmup, messages)
    File.write!(Path.join(results_dir, "rpc.md"), markdown)
    IO.puts("\n" <> markdown)
    :ok
  end

  defp run_cell(:c, pairs, base, warmup, messages) do
    paired_cell(:c, pairs, base, warmup, messages, fn mode, _index ->
      {native_binary("bench/c/build/bench_c_rpc"), ["--mode=#{mode}"]}
    end)
  end

  defp run_cell(:cpp, pairs, base, warmup, messages) do
    paired_cell(:cpp, pairs, base, warmup, messages, fn mode, _index ->
      {native_binary("bench/cpp/build/bench_cpp_rpc"), ["--mode=#{mode}"]}
    end)
  end

  defp run_cell(:go, pairs, base, warmup, messages) do
    paired_cell(:go, pairs, base, warmup, messages, fn mode, _index ->
      {native_binary("bench/go/build/rpc"), ["--mode=#{mode}"]}
    end)
  end

  defp run_cell(:rust, pairs, base, warmup, messages) do
    paired_cell(:rust, pairs, base, warmup, messages, fn mode, _index ->
      {native_binary("bench/rust/target/release/rpc"), ["--mode=#{mode}"]}
    end)
  end

  defp run_cell(:dotnet, pairs, base, warmup, messages) do
    paired_cell(:dotnet, pairs, base, warmup, messages, fn mode, _index ->
      {"dotnet", [native_binary("bench/dotnet/build/Rpc.dll"), "--mode=#{mode}"]}
    end)
  end

  defp run_cell(:java, pairs, base, warmup, messages) do
    args = java_classpath_args()

    paired_cell(:java, pairs, base, warmup, messages, fn mode, _index ->
      {"java", args ++ ["io.aeron.samples.bench.Rpc", "--mode=#{mode}"]}
    end)
  end

  defp run_cell(:python, pairs, base, warmup, messages) do
    paired_cell(:python, pairs, base, warmup, messages, fn
      "pong", _index -> {python_interpreter(), ["bench/python/rpc_pong.py"]}
      "ping", _index -> {python_interpreter(), ["bench/python/rpc_ping.py"]}
    end)
  end

  defp run_cell(:elixir, pairs, base, warmup, messages) do
    paired_cell(:elixir, pairs, base, warmup, messages, fn
      "pong", _index -> {"mix", ["run", "bench/elixir/rpc_pong.exs"]}
      "ping", _index -> {"mix", ["run", "bench/elixir/rpc_ping.exs"]}
    end)
  end

  defp run_cell(:elixir_invm, pairs, base, warmup, messages) do
    start_at_ns = shared_start_ns()
    env = Map.put(ping_env(base, 0, warmup, messages, start_at_ns), "BENCH_PAIRS", "#{pairs}")
    outputs = run_many([{"mix", ["run", "bench/elixir/rpc_pair.exs"], env}])
    finish(:elixir_invm, pairs, outputs, start_at_ns)
  end

  defp run_cell(:beam, pairs, base, warmup, messages) do
    start_at_ns = shared_start_ns()
    env = Map.put(ping_env(base, 0, warmup, messages, start_at_ns), "BENCH_PAIRS", "#{pairs}")
    outputs = run_many([{"mix", ["run", "bench/elixir/rpc_beam.exs"], env}])
    finish(:beam, pairs, outputs, start_at_ns)
  end

  defp paired_cell(client, pairs, base, warmup, messages, command_for) do
    pongers =
      for index <- 0..(pairs - 1) do
        {cmd, args} = command_for.("pong", index)
        start_background(cmd, args, rpc_env(base, index, warmup, messages))
      end

    Enum.each(pongers, &await_ready(&1, 120_000))
    start_at_ns = shared_start_ns()

    outputs =
      run_many(
        for index <- 0..(pairs - 1) do
          {cmd, args} = command_for.("ping", index)
          {cmd, args, ping_env(base, index, warmup, messages, start_at_ns)}
        end
      )

    stop_all(pongers)
    finish(client, pairs, outputs, start_at_ns)
  end

  defp shared_start_ns, do: System.os_time(:nanosecond) + @start_margin_ns

  defp finish(client, pairs, outputs, start_at_ns) do
    label = Atom.to_string(client)

    failures =
      outputs
      |> Enum.filter(fn {_output, status} -> status != 0 end)
      |> Enum.map(fn {output, status} ->
        "exit #{inspect(status)}: " <> String.slice(output, -600, 600)
      end)

    samples =
      outputs
      |> Enum.flat_map(fn {output, _status} -> parse_json_lines(output) end)

    cond do
      failures != [] ->
        failed(label, pairs, Enum.join(failures, "\n"))

      length(samples) != pairs ->
        failed(
          label,
          pairs,
          "expected #{pairs} result sets, parsed #{length(samples)}:\n" <>
            Enum.map_join(outputs, "\n---\n", fn {o, _} -> String.slice(o, -800, 800) end)
        )

      not overlapping?(samples) ->
        failed(label, pairs, "the pingers' timed loops did not all overlap")

      true ->
        aggregate(label, pairs, samples, start_at_ns)
    end
  end

  defp overlapping?(samples) do
    latest_start = samples |> Enum.map(& &1["started_at_ns"]) |> Enum.max()
    earliest_finish = samples |> Enum.map(& &1["finished_at_ns"]) |> Enum.min()
    latest_start < earliest_finish
  end

  defp aggregate(label, pairs, samples, start_at_ns) do
    p50s = samples |> Enum.map(& &1["p50_us"]) |> Enum.sort()
    p99s = samples |> Enum.map(& &1["p99_us"]) |> Enum.sort()
    p999s = samples |> Enum.map(& &1["p999_us"]) |> Enum.sort()
    means = Enum.map(samples, & &1["mean_us"])
    maxes = Enum.map(samples, & &1["max_us"])
    round_trips = samples |> Enum.map(& &1["samples"]) |> Enum.sum()
    starts = Enum.map(samples, & &1["started_at_ns"])
    first_start = Enum.min(starts)
    last_finish = samples |> Enum.map(& &1["finished_at_ns"]) |> Enum.max()
    window_ns = last_finish - first_start

    %{
      "client" => label,
      "pairs" => pairs,
      "status" => "ok",
      "p50_us" => median(p50s),
      "p99_us" => median(p99s),
      "p999_us" => median(p999s),
      "mean_us" => Enum.sum(means) / length(means),
      "max_us" => Enum.max(maxes),
      "round_trips_per_sec" => round_trips * 1.0e9 / window_ns,
      "window_ms" => window_ns / 1.0e6,
      "latest_start_ms" => (Enum.max(starts) - start_at_ns) / 1.0e6,
      "per_pair" => samples
    }
  end

  defp median(sorted) do
    count = length(sorted)
    Enum.at(sorted, div(count, 2))
  end

  defp failed(label, pairs, error) do
    IO.puts("FAILED: " <> String.slice(error, 0, 600))
    %{"client" => label, "pairs" => pairs, "status" => "failed", "error" => error}
  end

  defp parse_json_lines(output) do
    output
    |> String.split("\n")
    |> Enum.filter(&String.starts_with?(&1, "{\"client\":"))
    |> Enum.map(&Jason.decode!/1)
  end

  defp java_classpath_args do
    jar = java_jar()
    out = "bench/java/out"

    sources = ["bench/java/Rpc.java", "bench/java/PayloadPool.java"]

    if java_stale?(sources, Path.join(out, "io/aeron/samples/bench/Rpc.class")) do
      IO.puts("Compiling Java rpc bench...")
      {_, 0} = System.cmd("javac", ["-cp", jar, "-d", out | sources])
    end

    ["--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED", "-cp", "#{jar}:#{out}"]
  end

  defp java_stale?(sources, class) do
    not File.exists?(class) or
      Enum.any?(sources, &(File.stat!(&1).mtime > File.stat!(class).mtime))
  end

  defp rpc_env(base, index, warmup, messages) do
    %{
      "AERON_DIR" => aeron_dir(),
      "DYLD_LIBRARY_PATH" => aeron_lib_path(),
      "LD_LIBRARY_PATH" => aeron_lib_path(),
      "RPC_STREAM_BASE" => "#{base}",
      "RPC_PAIR_INDEX" => "#{index}",
      "RPC_MESSAGE_LENGTH" => "#{@message_length}",
      "RPC_WARMUP_MESSAGES" => "#{warmup}",
      "RPC_MESSAGES" => "#{messages}"
    }
  end

  defp ping_env(base, index, warmup, messages, start_at_ns),
    do: Map.put(rpc_env(base, index, warmup, messages), "RPC_START_AT_NS", "#{start_at_ns}")

  defp start_background(cmd, args, env) do
    port = open_port(cmd, args, env)
    {:os_pid, os_pid} = Port.info(port, :os_pid)
    {port, os_pid}
  end

  defp await_ready({port, _os_pid}, timeout_ms) do
    receive do
      {^port, {:data, data}} ->
        if String.contains?(data, "READY"), do: :ok, else: await_ready({port, nil}, timeout_ms)

      {^port, {:exit_status, status}} ->
        raise("ponger exited early with status #{status}")
    after
      timeout_ms -> raise("ponger never became ready")
    end
  end

  defp stop_all(processes) do
    Enum.each(processes, fn {port, os_pid} ->
      System.cmd("kill", ["-TERM", Integer.to_string(os_pid)])
      drain_port(port, 5_000)
    end)
  end

  defp drain_port(port, timeout_ms) do
    receive do
      {^port, {:data, _}} -> drain_port(port, timeout_ms)
      {^port, {:exit_status, _}} -> :ok
    after
      timeout_ms ->
        try do
          Port.close(port)
        rescue
          ArgumentError -> :ok
        end
    end
  end

  defp run_many(specs) do
    ports =
      Enum.map(specs, fn {cmd, args, env} ->
        port = open_port(cmd, args, env)
        {:os_pid, os_pid} = Port.info(port, :os_pid)
        {port, os_pid}
      end)

    collect_all(ports, Map.new(ports, fn {port, _} -> {port, {[], nil}} end), @cell_timeout_ms)
  end

  defp collect_all(ports, acc, timeout_ms) do
    if Enum.all?(acc, fn {_port, {_data, status}} -> status != nil end) do
      Enum.map(ports, fn {port, _} ->
        {data, status} = acc[port]
        {IO.iodata_to_binary(data), status}
      end)
    else
      receive do
        {port, {:data, data}} when is_map_key(acc, port) ->
          {existing, status} = acc[port]
          collect_all(ports, Map.put(acc, port, {[existing, data], status}), timeout_ms)

        {port, {:exit_status, status}} when is_map_key(acc, port) ->
          {existing, _} = acc[port]
          collect_all(ports, Map.put(acc, port, {existing, status}), timeout_ms)
      after
        timeout_ms ->
          Enum.each(ports, fn {port, os_pid} ->
            System.cmd("kill", ["-9", Integer.to_string(os_pid)])

            try do
              Port.close(port)
            rescue
              ArgumentError -> :ok
            end
          end)

          Enum.map(ports, fn {port, _} ->
            {data, status} = acc[port]
            {IO.iodata_to_binary([data, "\n[timed out]\n"]), status || :timeout}
          end)
      end
    end
  end

  defp open_port(cmd, args, env) do
    executable = System.find_executable(cmd) || cmd

    Port.open(
      {:spawn_executable, executable},
      [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        {:args, args},
        {:env, Enum.map(env, fn {k, v} -> {String.to_charlist(k), String.to_charlist(v)} end)}
      ]
    )
  end

  defp native_binary(relative), do: Path.expand(relative, File.cwd!())

  defp java_jar,
    do: Path.expand("~/.m2/repository/io/aeron/aeron-all/1.49.3/aeron-all-1.49.3.jar")

  defp python_interpreter, do: Path.expand("bench/python/.venv/bin/python", File.cwd!())
  defp aeron_dir, do: System.get_env("AERON_DIR", "/tmp/ae_drv")

  defp aeron_lib_path do
    System.get_env("DYLD_LIBRARY_PATH") || Path.expand("../aeron/build/lib", File.cwd!())
  end

  defp summary_line(%{"status" => "ok"} = cell) do
    "  p50=#{fmt(cell["p50_us"])}us p99=#{fmt(cell["p99_us"])}us p99.9=#{fmt(cell["p999_us"])}us mean=#{fmt(cell["mean_us"])}us rt/s=#{fmt_ops(cell["round_trips_per_sec"])}"
  end

  defp summary_line(_), do: "  failed"

  defp merge_existing(results, _results_dir, false), do: results

  defp merge_existing(results, results_dir, true) do
    fresh = MapSet.new(results, &cell_key/1)

    results_dir
    |> Path.join("rpc.json")
    |> File.read!()
    |> Jason.decode!()
    |> Map.fetch!("cells")
    |> Enum.reject(&MapSet.member?(fresh, cell_key(&1)))
    |> Kernel.++(results)
    |> Enum.sort_by(&{client_rank(&1["client"]), &1["pairs"]})
  end

  defp cell_key(cell), do: {cell["client"], cell["pairs"]}

  defp client_rank(client),
    do: Enum.find_index(@clients, &(Atom.to_string(&1) == client)) || length(@clients)

  defp write_json(results, results_dir) do
    path = Path.join(results_dir, "rpc.json")

    File.write!(
      path,
      Jason.encode!(%{run_at: DateTime.to_iso8601(DateTime.utc_now()), cells: results},
        pretty: true
      )
    )
  end

  defp render_markdown(results, pairs_list, warmup, messages) do
    single = Enum.filter(results, &(&1["pairs"] == 1))

    single_table =
      "| client | p50 µs | p90 µs | p99 µs | p99.9 µs | max µs | mean µs | round trips/s |\n|---|---:|---:|---:|---:|---:|---:|---:|\n" <>
        Enum.map_join(Enum.sort_by(single, &(&1["p50_us"] || 1.0e9)), "\n", &render_single_row/1)

    multi_table =
      "| client | " <>
        Enum.map_join(pairs_list, " | ", &"N=#{&1} rt/s") <>
        " | " <>
        Enum.map_join(pairs_list, " | ", &"N=#{&1} p50 µs") <>
        " |\n|---|" <>
        String.duplicate("---:|", length(pairs_list) * 2) <>
        "\n" <>
        (results
         |> Enum.group_by(& &1["client"])
         |> Enum.sort_by(fn {client, _} -> client end)
         |> Enum.map_join("\n", fn {client, cells} ->
           by_pairs = Map.new(cells, &{&1["pairs"], &1})

           rt =
             Enum.map(pairs_list, fn n ->
               cell_value(by_pairs[n], "round_trips_per_sec", &fmt_ops/1)
             end)

           p50 = Enum.map(pairs_list, fn n -> cell_value(by_pairs[n], "p50_us", &fmt/1) end)
           "| #{client} | " <> Enum.join(rt ++ p50, " | ") <> " |"
         end))

    """
    # Request/response round trip over `aeron:ipc`

    One-at-a-time request and response: the pinger sends a #{@message_length}-byte message and waits for its echo before sending the next. #{warmup} warmup round trips, then #{messages} measured round trips per pair.

    ## Single pair (two processes)

    #{single_table}

    ## N concurrent pairs — aggregate round trips/s over the shared timed window and median per-pair p50

    #{multi_table}

    ## Method

    #{method_text()}
    """
  end

  defp render_single_row(%{"status" => "ok"} = c) do
    "| #{c["client"]} | #{fmt(c["p50_us"])} | #{fmt(c["per_pair"] |> hd() |> Map.get("p90_us"))} | #{fmt(c["p99_us"])} | #{fmt(c["p999_us"])} | #{fmt(c["max_us"])} | #{fmt(c["mean_us"])} | #{fmt_ops(c["round_trips_per_sec"])} |"
  end

  defp render_single_row(c) do
    "| #{c["client"]} | failed: #{c["error"] |> String.split("\n") |> List.first() |> String.slice(0, 80)} | | | | | | |"
  end

  defp cell_value(nil, _key, _fmt), do: "—"
  defp cell_value(%{"status" => "failed"}, _key, _fmt), do: "failed"
  defp cell_value(cell, key, fmt), do: fmt.(cell[key])

  defp fmt(nil), do: "—"
  defp fmt(value), do: :io_lib.format("~.3f", [value * 1.0]) |> to_string()

  defp fmt_ops(nil), do: "—"

  defp fmt_ops(ops) when ops >= 1_000_000,
    do: :io_lib.format("~.2fM", [ops / 1_000_000]) |> to_string()

  defp fmt_ops(ops) when ops >= 1_000, do: :io_lib.format("~.1fK", [ops / 1_000]) |> to_string()
  defp fmt_ops(ops), do: :io_lib.format("~.0f", [ops * 1.0]) |> to_string()

  defp method_text do
    {ncpu, 0} = System.cmd("sysctl", ["-n", "hw.ncpu"])
    {perf, 0} = System.cmd("sysctl", ["-n", "hw.perflevel0.physicalcpu"])
    {eff, 0} = System.cmd("sysctl", ["-n", "hw.perflevel1.physicalcpu"])
    {brand, 0} = System.cmd("sysctl", ["-n", "machdep.cpu.brand_string"])

    """
    - Host: #{String.trim(brand)}, #{String.trim(ncpu)} logical cores (#{String.trim(perf)} performance + #{String.trim(eff)} efficiency), macOS, no CPU pinning. Media driver: C `aeronmd_s` at `#{aeron_dir()}`, default threading, shared by every cell. OTP #{:erlang.system_info(:otp_release)} / Elixir #{System.version()}.
    - Each pair uses two stream ids on `aeron:ipc` (ping and pong), unique per cell so lingering resources from a finished cell never collide with the next.
    - Every pinger runs its warmup, then waits until the wall clock reaches a start time the runner sets #{div(@start_margin_ns, 1_000_000_000)} s after launching the pingers, so the timed loops of all N pairs begin together. Pingers sleep until 2 ms before that time and spin for the rest, so waiting pingers leave the CPU to the ones still warming up; with more busy processes than cores some still start late, and `latest_start_ms` in `rpc.json` records how late the last one began. A cell fails if the pairs' timed loops do not all overlap. Each pinger reports the wall-clock start and finish of its timed loop. Round trips/s is the total measured round trips of all pairs divided by the time from the first start to the last finish. Per-pair p50 for N pairs is the median of the pairs' p50 values.
    - Every client is a program in this repository doing identical work, not an upstream sample. Per round trip the pinger encodes a 28-byte tick (little-endian u32 instrument id, i64 bid, i64 ask, i64 sequence) into the payload, offers it, spin-polls with a fragment limit of 1 until the echo arrives, and decodes the echo's four fields into a running sum. The ponger decodes every request the same way before echoing it back. Both sides print their sum on stderr so the decode cannot be optimized away.
    - Every message differs: each client builds the same pool of 4096 payloads up front from one splitmix64 byte stream (seed 0x5EEDAE20), and message `i` is pool entry `i mod 4096` with its tick stamped over the first 28 bytes, so every byte varies and the source is not always in L1.
    - `c`: `bench/c/rpc.c` (`bench_c_rpc --mode=ping|pong`), C client, busy-spin, two OS processes.
    - `cpp`: `bench/cpp/rpc.cpp` (`bench_cpp_rpc --mode=ping|pong`), C++ wrapper API, busy-spin, two OS processes.
    - `go`: `bench/go/cmd/rpc` (`rpc --mode=ping|pong`), one aergo client (pure Go, `github.com/andrewwormald/aergo` pinned in `bench/go/go.mod`). aergo has no conductor thread, so the program drives it: `DoWork()` while waiting for the first message (image discovery) and once every 1024 operations (keepalives); nothing else is added to the measured loop; busy-spin, two OS processes; the responder echoes the payload aergo copied out of the log.
    - `dotnet`: `bench/dotnet/Rpc` (`Rpc --mode=ping|pong`), Aeron.NET (`Aeron.Client` 1.52.2 from NuGet, pinned in `bench/dotnet/BenchKit/BenchKit.csproj`) on .NET 10; busy-spin, two OS processes; the responder echoes the log buffer directly.
    - `rust`: `bench/rust/src/bin/rpc.rs` (`rpc --mode=ping|pong`), rusteron-client 0.2.10, Rust bindings over the Aeron 1.52.2 C client it builds from source (pinned in `bench/rust/Cargo.toml`); busy-spin, two OS processes; the responder echoes the log buffer directly.
    - `java`: `bench/java/Rpc.java` (`io.aeron.samples.bench.Rpc --mode=ping|pong`) against aeron-all 1.49.3, BusySpinIdleStrategy, two JVMs.
    - `python`: `bench/python/rpc_ping.py` / `rpc_pong.py` with pyaeron (C client bindings), two OS processes, busy-spin; the pinger decodes the zero-copy `BufferView` with `struct.unpack_from`, the responder copies the payload out of the log (`tobytes`) before decoding and offering it back.
    - `elixir`: `bench/elixir/rpc_ping.exs` / `rpc_pong.exs`, two BEAM VMs each with its own `AeronElixir` client, direct handles, `Publisher.publish/3` and `Subscriber.next/1`, busy-spin; ticks are built as iodata and decoded with a binary pattern match. Round-trip timings are written into a preallocated `:atomics` array so the measured loop does not grow the pinger's heap.
    - `elixir_invm`: `bench/elixir/rpc_pair.exs`, pinger and ponger as two BEAM processes in one VM sharing one client (N pairs = 2N processes).
    - `beam`: two BEAM processes using `send`/`receive`, no Aeron; the same tick is built and decoded on both sides. Reference for in-VM messaging, not an Aeron client.
    """
  end
end

opts =
  System.argv()
  |> Enum.map(fn arg ->
    case String.split(arg, "=", parts: 2) do
      ["pairs", v] ->
        {:pairs, v |> String.split(",") |> Enum.map(&String.to_integer/1)}

      ["clients", v] ->
        {:clients, v |> String.split(",", trim: true) |> Enum.map(&String.to_atom/1)}

      ["merge", "true"] ->
        {:merge, true}

      [k, v] ->
        case Integer.parse(v) do
          {n, ""} -> {String.to_atom(k), n}
          _ -> {String.to_atom(k), v}
        end

      _ ->
        nil
    end
  end)
  |> Enum.reject(&is_nil/1)

AeronElixir.Bench.RpcRunner.run(opts)
