defmodule AeronElixir.Bench.Runner do
  @moduledoc false

  @clients ~w(elixir_raw python java c cpp go dotnet rust)a
  @scenarios ~w(latency throughput)a
  @default_sizes [32, 256, 1024]
  @default_warmup 3
  @default_time 10
  @default_term_length "16777216"

  def run(opts \\ []) do
    sizes = Keyword.get(opts, :sizes, @default_sizes)
    warmup = Keyword.get(opts, :warmup, @default_warmup)
    time_s = Keyword.get(opts, :time, @default_time)
    clients = Keyword.get(opts, :clients, @clients)
    scenarios = Keyword.get(opts, :scenarios, @scenarios)

    results_dir = Path.join(File.cwd!(), "bench/results")
    File.mkdir_p!(results_dir)

    cells = for s <- scenarios, c <- clients, size <- sizes, do: {s, c, size}
    total = length(cells)

    results =
      cells
      |> Enum.with_index(1)
      |> Enum.map(fn {{scenario, client, size}, idx} ->
        IO.puts(
          "\n[#{idx}/#{total}] #{scenario} #{client} #{size}B (warmup=#{warmup}s time=#{time_s}s)"
        )

        run_cell(scenario, client, size, warmup, time_s, results_dir)
      end)

    aggregate =
      results
      |> merge_existing(results_dir, Keyword.get(opts, :merge, false))
      |> build_aggregate()

    json_path = Path.join(results_dir, "summary.json")
    File.write!(json_path, Jason.encode!(aggregate, pretty: true))

    csv_path = Path.join(results_dir, "summary.csv")
    File.write!(csv_path, render_csv(aggregate))

    html_path = Path.join(results_dir, "summary.html")
    File.write!(html_path, render_html(aggregate))

    IO.puts("\n========== SUMMARY ==========")
    IO.puts(render_table(aggregate))
    IO.puts("\nWrote:")
    IO.puts("  #{json_path}")
    IO.puts("  #{csv_path}")
    IO.puts("  #{html_path}")

    :ok
  end

  defp run_cell(scenario, :beam, size, warmup, time_s, _results_dir) when is_atom(scenario) do
    scenario_str = Atom.to_string(scenario)

    env = %{
      "BENCH_PAYLOAD_SIZES" => "#{size}",
      "BENCH_WARMUP_S" => "#{warmup}",
      "BENCH_TIME_S" => "#{time_s}",
      # one message per item by default so the matrix is comparable to the
      # per-message Aeron/native columns; override to see batched framing.
      "BENCH_ITEMS_PER_MSG" => System.get_env("BENCH_ITEMS_PER_MSG", "1")
    }

    script = "bench/elixir/beam_#{scenario_str}.exs"
    {output, exit_code} = run_cell_command("mix", ["run", script], env, warmup, time_s)

    if exit_code != 0 do
      IO.puts("\nFAILED (exit #{inspect(exit_code)})")
      IO.puts(output)

      %{
        "scenario" => scenario_str,
        "client" => "beam",
        "payload_size" => size,
        "status" => "failed",
        "error" => output
      }
    else
      case output
           |> String.split("\n")
           |> Enum.find(&String.starts_with?(&1, "{\"client\":\"beam\"")) do
        nil ->
          %{
            "scenario" => scenario_str,
            "client" => "beam",
            "payload_size" => size,
            "status" => "failed",
            "error" => output
          }

        json_line ->
          json_line |> Jason.decode!() |> Map.put("status", "ok")
      end
    end
  end

  defp run_cell(scenario, client, size, warmup, time_s, _results_dir)
       when client in [:elixir, :elixir_list, :elixir_raw] and is_atom(scenario) do
    scenario_str = Atom.to_string(scenario)
    label = Atom.to_string(client)

    mode =
      case client do
        :elixir_raw -> "direct"
        :elixir_list -> "list"
        _ -> "public"
      end

    env = %{
      "BENCH_PAYLOAD_SIZES" => "#{size}",
      "BENCH_WARMUP_S" => "#{warmup}",
      "BENCH_TIME_S" => "#{time_s}",
      "BENCH_TERM_LENGTH" => term_length(),
      "BENCH_PUBLISH_MODE" => mode,
      "BENCH_CLIENT_LABEL" => label,
      "AERON_DIR" => System.get_env("AERON_DIR", "/tmp/ae_drv")
    }

    script = "bench/elixir/bench_#{scenario_str}.exs"

    {output, exit_code} = run_cell_command("mix", ["run", script], env, warmup, time_s)

    if exit_code != 0 do
      IO.puts("\nFAILED (exit #{exit_code})")
      IO.puts(output)

      %{
        "scenario" => scenario_str,
        "client" => label,
        "payload_size" => size,
        "status" => "failed",
        "error" => output
      }
    else
      case output
           |> String.split("\n")
           |> Enum.find(&String.starts_with?(&1, "{\"client\":\"#{label}\"")) do
        nil ->
          %{
            "scenario" => scenario_str,
            "client" => label,
            "payload_size" => size,
            "status" => "failed",
            "error" => output
          }

        json_line ->
          json_line
          |> Jason.decode!()
          |> Map.put("status", "ok")
      end
    end
  end

  defp run_cell(scenario, client, size, warmup, time_s, _results_dir)
       when client in [:python, :java, :c, :cpp, :go, :dotnet, :rust] do
    scenario_str = Atom.to_string(scenario)

    env = %{
      "AERON_DIR" => System.get_env("AERON_DIR", "/tmp/ae_drv"),
      "DYLD_LIBRARY_PATH" => aeron_lib_path(),
      "BENCH_TERM_LENGTH" => term_length()
    }

    {cmd, args} =
      case client do
        :java ->
          {"java", java_classpath_args() ++ ["io.aeron.samples.bench.Bench"]}

        :c ->
          {bench_binary(:c), []}

        :cpp ->
          {bench_binary(:cpp), []}

        :go ->
          {bench_binary(:go), []}

        :rust ->
          {bench_binary(:rust), []}

        :dotnet ->
          {"dotnet", [bench_binary(:dotnet)]}

        :python ->
          {python_interpreter(), ["bench/python/bench.py"]}
      end

    args =
      args ++
        ["--mode=#{scenario_str}", "--size=#{size}", "--warmup=#{warmup}", "--time=#{time_s}"]

    {output, exit_code} = run_cell_command(cmd, args, env, warmup, time_s)

    if exit_code != 0 do
      IO.puts("\nFAILED (exit #{exit_code})")
      IO.puts(output)

      %{
        "scenario" => scenario_str,
        "client" => Atom.to_string(client),
        "payload_size" => size,
        "status" => "failed",
        "error" => output
      }
    else
      case output |> String.split("\n") |> Enum.find(&String.starts_with?(&1, "{")) do
        nil ->
          %{
            "scenario" => scenario_str,
            "client" => Atom.to_string(client),
            "payload_size" => size,
            "status" => "failed",
            "error" => "no JSON"
          }

        json_line ->
          json_line
          |> Jason.decode!()
          |> Map.put("status", "ok")
      end
    end
  end

  defp run_cell_command(cmd, args, env, warmup, time_s) do
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

    collect_cell_output(port, os_pid, timeout_ms, [])
  end

  defp collect_cell_output(port, os_pid, timeout_ms, acc) do
    receive do
      {^port, {:data, data}} ->
        collect_cell_output(port, os_pid, timeout_ms, [acc, data])

      {^port, {:exit_status, status}} ->
        {IO.iodata_to_binary(acc), status}
    after
      timeout_ms ->
        if os_pid, do: System.cmd("kill", ["-9", Integer.to_string(os_pid)])
        close_cell_port(port)

        {IO.iodata_to_binary([acc, "\n[bench cell timed out after #{timeout_ms} ms — killed]\n"]),
         :timeout}
    end
  end

  defp close_cell_port(port) do
    Port.close(port)
  rescue
    ArgumentError -> :ok
  end

  defp java_classpath_args do
    jar = Path.expand("~/.m2/repository/io/aeron/aeron-all/1.49.3/aeron-all-1.49.3.jar")
    out = "bench/java/out"

    sources = ["bench/java/Bench.java", "bench/java/PayloadPool.java"]

    if java_stale?(sources, Path.join(out, "io/aeron/samples/bench/Bench.class")) do
      IO.puts("Compiling Java bench...")
      {_, 0} = System.cmd("javac", ["-cp", jar, "-d", out | sources])
    end

    ["--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED", "-cp", "#{jar}:#{out}"]
  end

  defp java_stale?(sources, class) do
    not File.exists?(class) or Enum.any?(sources, &(File.stat!(&1).mtime > File.stat!(class).mtime))
  end

  defp python_interpreter, do: Path.expand("bench/python/.venv/bin/python", File.cwd!())

  defp bench_binary(:c), do: Path.expand("bench/c/build/bench_c", File.cwd!())
  defp bench_binary(:cpp), do: Path.expand("bench/cpp/build/bench_cpp", File.cwd!())
  defp bench_binary(:go), do: Path.expand("bench/go/build/bench", File.cwd!())
  defp bench_binary(:dotnet), do: Path.expand("bench/dotnet/build/Bench.dll", File.cwd!())
  defp bench_binary(:rust), do: Path.expand("bench/rust/target/release/bench", File.cwd!())

  defp aeron_lib_path do
    System.get_env("DYLD_LIBRARY_PATH") || Path.expand("../aeron/build/lib", File.cwd!())
  end

  defp term_length, do: System.get_env("BENCH_TERM_LENGTH", @default_term_length)

  defp merge_existing(results, _results_dir, false), do: results

  defp merge_existing(results, results_dir, true) do
    fresh = MapSet.new(results, &cell_key/1)

    results_dir
    |> Path.join("summary.json")
    |> File.read!()
    |> Jason.decode!()
    |> Map.fetch!("cells")
    |> Enum.reject(&MapSet.member?(fresh, cell_key(&1)))
    |> Kernel.++(results)
    |> Enum.sort_by(&cell_order/1)
  end

  defp cell_key(cell), do: {cell["scenario"], cell["client"], cell["payload_size"]}

  defp cell_order(cell),
    do: {rank(@scenarios, cell["scenario"]), rank(@clients, cell["client"]), cell["payload_size"]}

  defp rank(names, name), do: Enum.find_index(names, &(Atom.to_string(&1) == name)) || length(names)

  defp build_aggregate(results) do
    %{
      run_at: DateTime.to_iso8601(DateTime.utc_now()),
      host: host_info(),
      cells: results
    }
  end

  defp host_info do
    {uname, _} = System.cmd("uname", ["-a"])

    %{
      os: String.trim(uname),
      beam: System.version(),
      otp: :erlang.system_info(:otp_release) |> to_string()
    }
  end

  defp render_table(%{cells: cells}) do
    cells
    |> Enum.group_by(& &1["scenario"])
    |> Enum.sort_by(fn {s, _} -> s end)
    |> Enum.map(fn {scenario, group} ->
      "\n--- #{String.upcase(scenario)} ---\n" <> render_scenario_group(group, scenario)
    end)
    |> Enum.join("\n")
  end

  defp render_scenario_group(cells, "latency") do
    rows =
      cells
      |> Enum.sort_by(&{&1["payload_size"], &1["client"]})
      |> Enum.map(fn c ->
        case c do
          %{"status" => "failed"} ->
            {c["payload_size"], c["client"], "FAILED", "", ""}

          %{} ->
            ops = c["ops_per_sec"] || 0.0
            median = c["median_us"] || 0.0

            {c["payload_size"], c["client"], format_ops(ops), format_us(median), format_ratio(c)}
        end
      end)

    rows
    |> Enum.chunk_by(fn {size, _, _, _, _} -> size end)
    |> Enum.map(fn group ->
      [head | _] = group
      size = elem(head, 0)

      lines =
        group
        |> Enum.map(fn {_, client, ops, median, _} ->
          "  #{pad(client, 8)}  #{pad(ops, 14)}  #{pad(median, 12)}"
        end)
        |> Enum.join("\n")

      "size=#{size}B\n  #{pad("client", 8)}  #{pad("ops/sec", 14)}  #{pad("median", 12)}\n#{lines}"
    end)
    |> Enum.join("\n\n")
  end

  defp render_scenario_group(cells, "throughput") do
    rows =
      cells
      |> Enum.sort_by(&{&1["payload_size"], &1["client"]})

    rows
    |> Enum.chunk_by(& &1["payload_size"])
    |> Enum.map(fn group ->
      [head | _] = group
      size = head["payload_size"]

      lines =
        group
        |> Enum.map(fn c ->
          case c do
            %{"status" => "failed"} ->
              "  #{pad(c["client"], 8)}  FAILED"

            %{} ->
              ops = c["ops_per_sec"] || 0.0
              bps = c["bytes_per_sec"] || ops * size

              "  #{pad(c["client"], 8)}  #{pad(format_ops(ops), 14)}  #{pad(format_bytes(bps), 12)}"
          end
        end)
        |> Enum.join("\n")

      "size=#{size}B\n  #{pad("client", 8)}  #{pad("ops/sec", 14)}  #{pad("bytes/sec", 12)}\n#{lines}"
    end)
    |> Enum.join("\n\n")
  end

  defp format_ratio(_), do: ""

  defp format_ops(ops) when ops >= 1_000_000,
    do: :io_lib.format("~.2fM", [ops / 1_000_000]) |> to_string()

  defp format_ops(ops) when ops >= 1_000,
    do: :io_lib.format("~.2fK", [ops / 1_000]) |> to_string()

  defp format_ops(ops), do: :io_lib.format("~.2f", [ops]) |> to_string()

  defp format_us(us) when is_integer(us), do: format_us(us / 1.0)
  defp format_us(us) when us >= 1000, do: :io_lib.format("~.2fms", [us / 1000]) |> to_string()
  defp format_us(us), do: :io_lib.format("~.2fus", [us]) |> to_string()

  defp format_bytes(bps) when is_integer(bps), do: format_bytes(bps / 1.0)

  defp format_bytes(bps) when bps >= 1_000_000_000,
    do: :io_lib.format("~.2fGB/s", [bps / 1_000_000_000]) |> to_string()

  defp format_bytes(bps) when bps >= 1_000_000,
    do: :io_lib.format("~.2fMB/s", [bps / 1_000_000]) |> to_string()

  defp format_bytes(bps) when bps >= 1_000,
    do: :io_lib.format("~.2fKB/s", [bps / 1_000]) |> to_string()

  defp format_bytes(bps), do: :io_lib.format("~.2fB/s", [bps]) |> to_string()

  defp pad(s, width) when is_binary(s), do: String.pad_trailing(s, width)
  defp pad(s, width), do: pad(to_string(s), width)

  defp render_csv(%{cells: cells}) do
    header =
      "scenario,client,payload_size,ops_per_sec,bytes_per_sec,median_us,p99_us,mean_us,status\n"

    rows =
      cells
      |> Enum.map(fn c ->
        [
          c["scenario"],
          c["client"],
          c["payload_size"],
          c["ops_per_sec"],
          c["bytes_per_sec"],
          c["median_us"],
          c["p99_us"],
          c["mean_us"],
          c["status"]
        ]
        |> Enum.join(",")
      end)
      |> Enum.join("\n")

    header <> rows <> "\n"
  end

  defp render_html(%{run_at: run_at, host: host, cells: cells}) do
    cells_by_scenario = Enum.group_by(cells, & &1["scenario"])

    sections =
      ["latency", "throughput"]
      |> Enum.map(fn scenario ->
        group = Map.get(cells_by_scenario, scenario, [])
        render_html_section(scenario, group)
      end)
      |> Enum.join("\n")

    """
    <!doctype html>
    <html><head>
    <meta charset="utf-8">
    <title>Aeron Elixir Benchmark — #{run_at}</title>
    <style>
      body { font-family: -apple-system, system-ui, sans-serif; margin: 2rem; }
      table { border-collapse: collapse; margin-bottom: 2rem; }
      th, td { border: 1px solid #ccc; padding: 0.4rem 0.8rem; text-align: right; }
      th:first-child, td:first-child { text-align: left; }
      th { background: #f0f0f0; }
      .client-elixir { background: #fde8e8; }
      .failed { color: #c00; font-style: italic; }
      h2 { border-bottom: 2px solid #888; padding-bottom: 0.3rem; }
      .meta { color: #666; font-size: 0.9rem; margin-bottom: 1rem; }
    </style>
    </head><body>
    <h1>Aeron Elixir Benchmark</h1>
    <div class="meta">Run at #{run_at} — OTP #{host.otp} / Elixir #{host.beam}<br>#{host.os}</div>
    #{sections}
    </body></html>
    """
  end

  defp render_html_section(scenario, cells) do
    by_size = Enum.group_by(cells, & &1["payload_size"])

    rows =
      by_size
      |> Enum.sort_by(fn {size, _} -> size end)
      |> Enum.flat_map(fn {size, group} ->
        Enum.sort_by(group, & &1["client"])
        |> Enum.map(fn c ->
          client = c["client"]

          case c do
            %{"status" => "failed"} ->
              ~s|<tr class="client-#{client}"><td>#{size}</td><td>#{client}</td><td colspan="6" class="failed">failed</td></tr>|

            %{} ->
              ops = c["ops_per_sec"]
              median = c["median_us"]
              p99 = c["p99_us"]
              mean = c["mean_us"]

              bytes_cell =
                case scenario do
                  "throughput" -> "<td>#{format_bytes(c["bytes_per_sec"] || 0)}</td>"
                  _ -> "<td>—</td>"
                end

              ~s|<tr class="client-#{client}"><td>#{size}</td><td>#{client}</td><td>#{format_ops(ops)}</td>#{bytes_cell}<td>#{format_us(mean || 0)}</td><td>#{format_us(median || 0)}</td><td>#{format_us(p99 || 0)}</td></tr>|
          end
        end)
      end)
      |> Enum.join("\n")

    title = scenario |> String.upcase()

    bytes_header =
      if scenario == "throughput" do
        "<th>bytes/sec</th>"
      else
        ""
      end

    """
    <h2>#{title}</h2>
    <table>
    <thead><tr><th>payload (B)</th><th>client</th><th>ops/sec</th>#{bytes_header}<th>mean</th><th>median</th><th>p99</th></tr></thead>
    <tbody>
    #{rows}
    </tbody>
    </table>
    """
  end
end

opts =
  case System.argv() do
    [] ->
      []

    args ->
      args
      |> Enum.map(fn a ->
        case String.split(a, "=", parts: 2) do
          ["sizes", v] ->
            {:sizes, v |> String.split(",") |> Enum.map(&String.to_integer/1)}

          ["clients", v] ->
            {:clients, v |> String.split(",", trim: true) |> Enum.map(&String.to_atom/1)}

          ["scenarios", v] ->
            {:scenarios, v |> String.split(",") |> Enum.map(&String.to_atom/1)}

          [k, "true"] ->
            {String.to_atom(k), true}

          [k, "false"] ->
            {String.to_atom(k), false}

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
  end

AeronElixir.Bench.Runner.run(opts)
