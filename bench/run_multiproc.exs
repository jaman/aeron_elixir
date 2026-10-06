defmodule AeronElixir.Bench.MultiprocRunner do
  @moduledoc false

  @clients ~w(elixir python java c cpp go dotnet rust)a
  @default_workers [1, 2, 4, 8]
  @default_sizes [256]
  @default_warmup 2
  @default_time 5
  @default_term_length "16777216"

  def run(opts \\ []) do
    workers = Keyword.get(opts, :workers, @default_workers)
    sizes = Keyword.get(opts, :sizes, @default_sizes)
    warmup = Keyword.get(opts, :warmup, @default_warmup)
    time_s = Keyword.get(opts, :time, @default_time)
    clients = Keyword.get(opts, :clients, @clients)

    results_dir = Path.join(File.cwd!(), "bench/results")
    File.mkdir_p!(results_dir)

    cells = for client <- clients, size <- sizes, n <- workers, do: {client, size, n}
    total = length(cells)

    results =
      cells
      |> Enum.with_index(1)
      |> Enum.map(fn {{client, size, n}, idx} ->
        IO.puts(
          "\n[#{idx}/#{total}] #{client} workers=#{n} #{size}B (warmup=#{warmup}s time=#{time_s}s)"
        )

        run_cell(client, size, n, warmup, time_s)
      end)

    write_json(results, results_dir)
    merged = merge_existing(results, results_dir, Keyword.get(opts, :merge, false))
    markdown = render_markdown(merged, workers, sizes, warmup, time_s)
    File.write!(Path.join(results_dir, "multiproc.md"), markdown)

    IO.puts("\n" <> markdown)
    :ok
  end

  defp run_cell(client, size, n, warmup, time_s)
       when client in [:elixir, :elixir_list, :beam] do
    env = %{
      "BENCH_PAIRS" => "#{n}",
      "BENCH_PAYLOAD_SIZES" => "#{size}",
      "BENCH_WARMUP_S" => "#{warmup}",
      "BENCH_TIME_S" => "#{time_s}",
      "BENCH_TERM_LENGTH" => term_length(),
      "BENCH_PUBLISH_MODE" => publish_mode(client),
      "AERON_DIR" => aeron_dir()
    }

    {output, exit_code} = run_command("mix", ["run", elixir_script(client)], env, warmup, time_s)

    parse_result(client, size, n, output, exit_code)
  end

  defp run_cell(client, size, n, warmup, time_s) when client in [:python, :java, :c, :cpp, :go, :dotnet, :rust] do
    env = %{
      "AERON_DIR" => aeron_dir(),
      "DYLD_LIBRARY_PATH" => aeron_lib_path(),
      "BENCH_TERM_LENGTH" => term_length()
    }

    {cmd, args} =
      case client do
        :python -> {python_interpreter(), ["bench/python/bench_multiproc.py"]}
        :java -> {"java", java_classpath_args() ++ ["io.aeron.samples.bench.BenchMultiproc"]}
        :c -> {Path.expand("bench/c/build/bench_c_multiproc", File.cwd!()), []}
        :cpp -> {Path.expand("bench/cpp/build/bench_cpp_multiproc", File.cwd!()), []}
        :go -> {Path.expand("bench/go/build/bench_multiproc", File.cwd!()), []}
        :dotnet -> {"dotnet", [Path.expand("bench/dotnet/build/BenchMultiproc.dll", File.cwd!())]}
        :rust -> {Path.expand("bench/rust/target/release/bench_multiproc", File.cwd!()), []}
      end

    args = args ++ ["--workers=#{n}", "--size=#{size}", "--warmup=#{warmup}", "--time=#{time_s}"]
    {output, exit_code} = run_command(cmd, args, env, warmup, time_s)
    parse_result(client, size, n, output, exit_code)
  end

  defp publish_mode(:elixir_list), do: "list"
  defp publish_mode(_), do: "per_message"

  defp elixir_script(:beam), do: "bench/elixir/beam_multiproc.exs"
  defp elixir_script(_), do: "bench/elixir/aeron_multiproc.exs"

  defp parse_result(client, size, n, output, 0) do
    label = Atom.to_string(client)

    case output |> String.split("\n") |> Enum.find(&String.starts_with?(&1, "{\"client\":")) do
      nil ->
        failed(client, size, n, "no JSON line in output:\n" <> output)

      json_line ->
        json_line
        |> Jason.decode!()
        |> Map.put("client", label)
        |> Map.put("status", "ok")
    end
  end

  defp parse_result(client, size, n, output, exit_code) do
    failed(client, size, n, "exit #{inspect(exit_code)}:\n" <> output)
  end

  defp failed(client, size, n, error) do
    IO.puts("FAILED: " <> String.slice(error, 0, 400))

    %{
      "client" => Atom.to_string(client),
      "payload_size" => size,
      "workers" => n,
      "status" => "failed",
      "error" => error
    }
  end

  defp run_command(cmd, args, env, warmup, time_s) do
    timeout_ms = (warmup + time_s + 60) * 1000
    executable = System.find_executable(cmd) || cmd

    port =
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

    os_pid =
      case Port.info(port, :os_pid) do
        {:os_pid, pid} -> pid
        _ -> nil
      end

    collect_output(port, os_pid, timeout_ms, [])
  end

  defp collect_output(port, os_pid, timeout_ms, acc) do
    receive do
      {^port, {:data, data}} ->
        collect_output(port, os_pid, timeout_ms, [acc, data])

      {^port, {:exit_status, status}} ->
        {IO.iodata_to_binary(acc), status}
    after
      timeout_ms ->
        if os_pid, do: System.cmd("kill", ["-9", Integer.to_string(os_pid)])
        close_port(port)

        {IO.iodata_to_binary([acc, "\n[cell timed out after #{timeout_ms} ms — killed]\n"]),
         :timeout}
    end
  end

  defp close_port(port) do
    Port.close(port)
  rescue
    ArgumentError -> :ok
  end

  defp java_classpath_args do
    jar = Path.expand("~/.m2/repository/io/aeron/aeron-all/1.49.3/aeron-all-1.49.3.jar")
    out = "bench/java/out"

    sources = ["bench/java/BenchMultiproc.java", "bench/java/PayloadPool.java"]

    if java_stale?(sources, Path.join(out, "io/aeron/samples/bench/BenchMultiproc.class")) do
      IO.puts("Compiling Java multiproc bench...")
      {_, 0} = System.cmd("javac", ["-cp", jar, "-d", out | sources])
    end

    ["--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED", "-cp", "#{jar}:#{out}"]
  end

  defp java_stale?(sources, class) do
    not File.exists?(class) or Enum.any?(sources, &(File.stat!(&1).mtime > File.stat!(class).mtime))
  end

  defp python_interpreter, do: Path.expand("bench/python/.venv/bin/python", File.cwd!())

  defp aeron_dir, do: System.get_env("AERON_DIR", "/tmp/ae_drv")

  defp aeron_lib_path do
    System.get_env("DYLD_LIBRARY_PATH") || Path.expand("../aeron/build/lib", File.cwd!())
  end

  defp term_length, do: System.get_env("BENCH_TERM_LENGTH", @default_term_length)

  defp merge_existing(results, _results_dir, false), do: results

  defp merge_existing(results, results_dir, true) do
    fresh = MapSet.new(results, &cell_key/1)

    results_dir
    |> Path.join("multiproc-*.json")
    |> Path.wildcard()
    |> Enum.flat_map(&(&1 |> File.read!() |> Jason.decode!() |> Map.fetch!("cells")))
    |> Enum.reject(&MapSet.member?(fresh, cell_key(&1)))
    |> Kernel.++(results)
    |> Enum.sort_by(&{client_rank(&1["client"]), &1["payload_size"], &1["workers"]})
  end

  defp cell_key(cell), do: {cell["client"], cell["payload_size"], cell["workers"]}

  defp client_rank(client),
    do: Enum.find_index(@clients, &(Atom.to_string(&1) == client)) || length(@clients)

  defp write_json(results, results_dir) do
    results
    |> Enum.group_by(& &1["client"])
    |> Enum.each(fn {client, cells} ->
      path = Path.join(results_dir, "multiproc-#{client}.json")

      File.write!(
        path,
        Jason.encode!(%{run_at: DateTime.to_iso8601(DateTime.utc_now()), cells: cells},
          pretty: true
        )
      )
    end)
  end

  defp render_markdown(results, workers, sizes, warmup, time_s) do
    by_size = Enum.group_by(results, & &1["payload_size"])

    tables =
      sizes
      |> Enum.map(fn size ->
        cells = Map.get(by_size, size, [])
        render_size_table(size, cells, workers)
      end)
      |> Enum.join("\n\n")

    """
    # Multi-worker IPC throughput

    #{tables}

    ## Method

    #{method_text(warmup, time_s)}
    """
  end

  defp render_size_table(size, cells, workers) do
    header =
      "| client | " <>
        Enum.map_join(workers, " | ", &"N=#{&1}") <>
        " | peak | scaling N=1→peak |\n|---|" <>
        String.duplicate("---:|", length(workers) + 2)

    rows =
      cells
      |> Enum.group_by(& &1["client"])
      |> Enum.sort_by(fn {client, _} -> client end)
      |> Enum.map(fn {client, client_cells} ->
        by_workers = Map.new(client_cells, &{&1["workers"], &1})
        base = by_workers[List.first(workers)]
        top = by_workers[List.last(workers)]

        peak =
          client_cells
          |> Enum.filter(&(&1["status"] == "ok"))
          |> Enum.max_by(& &1["ops_per_sec"], fn -> top end)

        columns = Enum.map(workers, fn n -> format_cell(by_workers[n]) end)
        scaling = format_scaling(base, peak)
        peak_label = if peak, do: "N=#{peak["workers"]}", else: "—"

        "| #{client} | " <> Enum.join(columns, " | ") <> " | #{peak_label} | #{scaling} |"
      end)

    "## #{size} B payload — aggregate msgs/sec\n\n" <> header <> "\n" <> Enum.join(rows, "\n")
  end

  defp format_cell(%{"status" => "ok"} = cell) do
    received_note =
      if cell["received"] == cell["samples"],
        do: "",
        else: " ⚠︎ recv #{cell["received"]}/#{cell["samples"]}"

    format_ops(cell["ops_per_sec"]) <> received_note
  end

  defp format_cell(%{"status" => "failed", "error" => error}) do
    "failed (" <> (error |> String.split("\n") |> List.first() |> String.slice(0, 60)) <> ")"
  end

  defp format_cell(nil), do: "—"

  defp format_scaling(%{"status" => "ok", "ops_per_sec" => base}, %{
         "status" => "ok",
         "ops_per_sec" => top
       })
       when base > 0 do
    :io_lib.format("~.2fx", [top / base]) |> to_string()
  end

  defp format_scaling(_, _), do: "—"

  defp format_ops(ops) when ops >= 1_000_000,
    do: :io_lib.format("~.2fM", [ops / 1_000_000]) |> to_string()

  defp format_ops(ops) when ops >= 1_000,
    do: :io_lib.format("~.2fK", [ops / 1_000]) |> to_string()

  defp format_ops(ops), do: :io_lib.format("~.2f", [ops]) |> to_string()

  defp method_text(warmup, time_s) do
    {ncpu, 0} = System.cmd("sysctl", ["-n", "hw.ncpu"])
    {perf, 0} = System.cmd("sysctl", ["-n", "hw.perflevel0.physicalcpu"])
    {eff, 0} = System.cmd("sysctl", ["-n", "hw.perflevel1.physicalcpu"])
    {brand, 0} = System.cmd("sysctl", ["-n", "machdep.cpu.brand_string"])
    {uname, 0} = System.cmd("uname", ["-a"])

    """
    - Host: #{String.trim(brand)}, #{String.trim(ncpu)} logical cores (#{String.trim(perf)} performance + #{String.trim(eff)} efficiency). `#{String.trim(uname)}`. OTP #{:erlang.system_info(:otp_release)} / Elixir #{System.version()}.
    - Each worker owns one publication and one subscription on a private `aeron:ipc` channel (term-length #{term_length()}) against the shared media driver at `#{aeron_dir()}`. Workers never share a log buffer, so the driver only does conductor work.
    - Loop per worker: offer 1000 messages, polling the subscription (limit 1024) whenever the publication back-pressures, then poll once more; run for #{time_s} s after #{warmup} s warmup. Aggregate ops/sec = total sent across workers ÷ the slowest worker's elapsed time. The receive count is checked against the send count for every worker; a mismatch is flagged in the table.
    - Per-message work is the same for every Aeron client: encode a 28-byte tick (u32 instrument id, i64 bid, i64 ask, i64 sequence) into the payload, offer it, and on receive decode the four fields and fold them into a running sum. Each cell prints its tick sum on stderr.
    - Every message differs: each client builds the same pool of 4096 payloads up front from one splitmix64 byte stream (seed 0x5EEDAE20), and message `i` is pool entry `i mod 4096` with its tick stamped over the first 28 bytes, so every byte varies and the source is not always in L1.
    - `elixir`: N BEAM processes in one VM sharing one `AeronElixir` client, direct handles (`AeronElixir.publication_handle/1`, `subscription_handles/1`), per-message `Publisher.publish/3`, drain with `Subscriber.collect/2` and decode every payload.
    - `elixir_list`: same work as `elixir` (a distinct tick encoded per message, every payload decoded on receive), but the send side hands each batch of 1000 payloads to `Publisher.publish_list/3` as one native call. Equal work, amortized crossing.
    - `beam`: N producer/consumer pairs of BEAM processes using `send`/`receive`, one tick per message, consumer decodes. No Aeron; reference for in-VM messaging.
    - `python`: N OS processes via `multiprocessing` (spawn), one `pyaeron.Aeron` client each; per-message `offer` with the fragment handler decoding the tick fields.
    - `java`: N threads sharing one `Aeron` client (1.49.3); per-message `offer` with a decoding `FragmentHandler`.
    - `c`: N pthreads, one `aeron_t` client per thread (libaeron from ../aeron/build); per-message `aeron_publication_offer` with a decoding fragment handler.
    - `cpp`: N `std::thread`s, one C++ wrapper `Aeron` client per thread; per-message `Publication::offer` with a decoding lambda handler passed to the template `Subscription::poll`.
    - `go`: N goroutines, one aergo client per goroutine (pure Go, `github.com/andrewwormald/aergo` pinned in `bench/go/go.mod`); per-message `Publication.Offer` with a decoding fragment handler. aergo has no conductor thread, so each goroutine also calls `DoWork()` while waiting for its first message (image discovery) and once every 1024 operations (keepalives); nothing else is added to the measured loop.
    - `dotnet`: N threads sharing one Aeron.NET client, as the Java threads do (`Aeron.Client` 1.52.2 from NuGet on .NET 10, pinned in `bench/dotnet/BenchKit/BenchKit.csproj`); per-message `Publication.Offer` with a decoding `FragmentHandler`.
    - `rust`: N threads, one client per thread as in C (rusteron-client 0.2.10, Rust bindings over the Aeron 1.52.2 C client it builds from source (pinned in `bench/rust/Cargo.toml`)); per-message `offer` with a decoding fragment handler.
    """
  end
end

opts =
  System.argv()
  |> Enum.map(fn arg ->
    case String.split(arg, "=", parts: 2) do
      ["workers", v] ->
        {:workers, v |> String.split(",") |> Enum.map(&String.to_integer/1)}

      ["sizes", v] ->
        {:sizes, v |> String.split(",") |> Enum.map(&String.to_integer/1)}

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

AeronElixir.Bench.MultiprocRunner.run(opts)
