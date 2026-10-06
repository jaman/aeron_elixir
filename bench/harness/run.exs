defmodule Harness.Runner do
  @moduledoc false

  @clients ~w(elixir_direct python java c go dotnet rust)a
  @default_rates [250_000, 500_000, 1_000_000, 2_000_000]
  @default_duration 10
  @default_repeats 3
  @repeat_stream_stride 500
  @stream_base 9200

  def run(opts) do
    clients = Keyword.get(opts, :clients, @clients)
    rates = Keyword.get(opts, :rates, @default_rates)
    settings = %{
      duration: Keyword.get(opts, :duration, @default_duration),
      repeats: Keyword.get(opts, :repeats, @default_repeats)
    }

    results_dir = Path.join(File.cwd!(), "bench/results")
    File.mkdir_p!(results_dir)

    {clients, saturation, stepped} =
      merge_existing(
        clients,
        run_suite(clients, [:max], settings, 0),
        run_stepped(clients, rates, settings, length(clients)),
        results_dir,
        Keyword.get(opts, :merge, false)
      )

    write_json(results_dir, saturation ++ stepped)
    write_markdown(results_dir, clients, rates, settings, saturation, stepped)

    IO.puts("\n========== SATURATION ==========")
    IO.puts(render_saturation(saturation))
    IO.puts("\n========== RATE STEPS ==========")
    IO.puts(render_steps(stepped, rates))
    IO.puts("\nWrote #{Path.join(results_dir, "harness.md")}")
  end

  defp run_stepped(clients, rates, settings, offset) do
    {cells, _offset} =
      Enum.reduce(clients, {[], offset}, fn client, {acc, index} ->
        {cells, index} = stepped_for_client(client, rates, settings, index)
        {acc ++ cells, index}
      end)

    cells
  end

  defp stepped_for_client(client, rates, settings, index) do
    Enum.reduce(rates, {[], index}, fn rate, {acc, index} ->
      step_cell(acc, client, rate, settings, index, keeping_up?(acc))
    end)
  end

  defp keeping_up?([]), do: true

  defp keeping_up?(cells) do
    last = List.last(cells)
    last.status == "ok" and last.consumed_ratio >= 0.98
  end

  defp step_cell(acc, client, rate, _settings, index, false) do
    {acc ++ [%{client: client, rate: rate, status: "skipped", consumed_ratio: 0.0}], index}
  end

  defp step_cell(acc, client, rate, settings, index, true) do
    cell = run_cell(client, rate, settings, index)
    {acc ++ [cell], index + 1}
  end

  defp run_suite(clients, rates, settings, offset) do
    clients
    |> Enum.with_index(offset)
    |> Enum.map(fn {client, index} ->
      Enum.map(rates, fn rate -> run_cell(client, rate, settings, index) end)
    end)
    |> List.flatten()
  end

  defp run_cell(client, rate, settings, index) do
    1..settings.repeats
    |> Enum.map(&run_once(client, rate, settings.duration, index, &1))
    |> median_run(rate)
  end

  defp median_run(runs, rate) do
    case Enum.filter(runs, &(&1.status == "ok")) do
      [] ->
        hd(runs)

      ok_runs ->
        ok_runs
        |> Enum.sort_by(&median_key(&1, rate))
        |> Enum.at(div(length(ok_runs) - 1, 2))
        |> Map.merge(%{
          runs: length(ok_runs),
          p99_runs: Enum.map(ok_runs, & &1.p99),
          consume_rate_runs: Enum.map(ok_runs, & &1.consume_rate)
        })
    end
  end

  defp median_key(run, :max), do: run.consume_rate
  defp median_key(run, _rate), do: run.p99

  defp run_once(client, rate, duration, index, repeat) do
    stream = @stream_base + repeat * @repeat_stream_stride + index * 7 + :erlang.phash2({client, rate}, 5)
    IO.puts("\n--- #{client} rate=#{rate} run=#{repeat} stream=#{stream} ---")

    env = %{
      "AERON_DIR" => aeron_dir(),
      "HARNESS_CHANNEL" => channel(),
      "HARNESS_STREAM" => "#{stream}",
      "HARNESS_DURATION_S" => "#{duration}",
      "HARNESS_RATE" => "#{rate}",
      "HARNESS_LABEL" => "#{client}",
      "HARNESS_MODE" => mode_for(client),
      "DYLD_LIBRARY_PATH" => aeron_lib_path(),
      "LD_LIBRARY_PATH" => aeron_lib_path()
    }

    consumer = start_consumer(client, env)
    await_ready(consumer)

    publisher_output = run_publisher(env, duration)
    consumer_output = await_consumer(consumer, duration)

    build_cell(client, rate, publisher_output, consumer_output)
  end

  defp mode_for(:elixir_public), do: "public"
  defp mode_for(_client), do: "direct"

  defp start_consumer(client, env) do
    {command, args} = consumer_command(client)
    port_open(command, args, env)
  end

  defp consumer_command(:elixir_direct), do: {"mix", ["run", "bench/harness/consumer.exs"]}
  defp consumer_command(:elixir_public), do: {"mix", ["run", "bench/harness/consumer.exs"]}
  defp consumer_command(:python), do: {python_interpreter(), ["bench/harness/consumer.py"]}
  defp consumer_command(:c), do: {c_consumer_binary(), []}
  defp consumer_command(:go), do: {Path.expand("bench/go/build/consumer", File.cwd!()), []}
  defp consumer_command(:rust), do: {Path.expand("bench/rust/target/release/consumer", File.cwd!()), []}
  defp consumer_command(:dotnet), do: {"dotnet", [Path.expand("bench/dotnet/build/Consumer.dll", File.cwd!())]}

  defp consumer_command(:java) do
    {"java", java_args() ++ ["io.aeron.samples.bench.Consumer"]}
  end

  defp run_publisher(env, duration) do
    port = port_open(c_price_server_binary(), [], env)
    collect(port, (duration + 90) * 1000, [])
  end

  defp await_ready(port) do
    receive do
      {^port, {:data, data}} ->
        ready_or_wait(String.contains?(data, "READY"), port)
    after
      120_000 -> IO.puts("consumer never reported READY")
    end
  end

  defp ready_or_wait(true, _port), do: :ok
  defp ready_or_wait(false, port), do: await_ready(port)

  defp await_consumer(port, duration) do
    collect(port, (duration + 90) * 1000, [])
  end

  defp port_open(command, args, env) do
    executable = System.find_executable(command) || command

    Port.open({:spawn_executable, executable}, [
      :binary,
      :exit_status,
      {:args, args},
      {:env, Enum.map(env, fn {k, v} -> {String.to_charlist(k), String.to_charlist(v)} end)}
    ])
  end

  defp collect(port, timeout, acc) do
    receive do
      {^port, {:data, data}} -> collect(port, timeout, [acc, data])
      {^port, {:exit_status, _status}} -> IO.iodata_to_binary(acc)
    after
      timeout ->
        kill_port(port)
        IO.iodata_to_binary([acc, "\n[timeout]\n"])
    end
  end

  defp kill_port(port) do
    case Port.info(port, :os_pid) do
      {:os_pid, pid} -> System.cmd("kill", ["-9", "#{pid}"])
      _ -> :ok
    end

    close_port(port)
  end

  defp close_port(port) do
    Port.close(port)
  rescue
    ArgumentError -> :ok
  end

  defp build_cell(client, rate, publisher_output, consumer_output) do
    publisher = decode_line(publisher_output, "\"role\":\"publisher\"")
    consumer = decode_line(consumer_output, "\"role\":\"consumer\"")

    assemble(client, rate, publisher, consumer)
  end

  defp assemble(client, rate, nil, _consumer) do
    %{client: client, rate: rate, status: "publisher failed", consumed_ratio: 0.0}
  end

  defp assemble(client, rate, _publisher, nil) do
    %{client: client, rate: rate, status: "consumer failed", consumed_ratio: 0.0}
  end

  defp assemble(client, rate, publisher, consumer) do
    published = publisher["published"]
    consumed = consumer["consumed"]

    %{
      client: client,
      rate: rate,
      status: "ok",
      published: published,
      consumed: consumed,
      achieved_rate: publisher["achieved_rate"],
      consume_rate: consumed * publisher["achieved_rate"] / max(published, 1),
      consumed_ratio: consumed / max(published, 1),
      lag: published - consumed,
      gaps: consumer["gaps"],
      back_pressured_ms: publisher["back_pressured_ms"],
      p50: consumer["p50_us"],
      p90: consumer["p90_us"],
      p99: consumer["p99_us"],
      p999: consumer["p999_us"],
      max: consumer["max_us"],
      mean: consumer["mean_us"],
      negatives: consumer["negative_latencies"]
    }
  end

  defp decode_line(output, marker) do
    output
    |> String.split("\n")
    |> Enum.find(&String.contains?(&1, marker))
    |> decode_json()
  end

  defp decode_json(nil), do: nil
  defp decode_json(line), do: Jason.decode!(line)

  defp render_saturation(cells) do
    header = "  #{pad("client", 16)}#{pad("published", 14)}#{pad("consumed", 14)}#{pad("consume/s", 14)}#{pad("lag", 12)}#{pad("p50 us", 10)}#{pad("p99 us", 10)}"

    rows =
      Enum.map_join(cells, "\n", fn cell -> saturation_row(cell) end)

    header <> "\n" <> rows
  end

  defp saturation_row(%{status: "ok"} = cell) do
    "  #{pad(cell.client, 16)}#{pad(millions(cell.published), 14)}#{pad(millions(cell.consumed), 14)}#{pad(millions(cell.consume_rate), 14)}#{pad(millions(cell.lag), 12)}#{pad(us(cell.p50), 10)}#{pad(us(cell.p99), 10)}"
  end

  defp saturation_row(cell), do: "  #{pad(cell.client, 16)}#{cell.status}"

  defp render_steps(cells, rates) do
    header = "  #{pad("client", 16)}" <> Enum.map_join(rates, "", &pad("#{div(&1, 1000)}k", 22))

    grouped = Enum.group_by(cells, & &1.client)

    rows =
      Enum.map_join(grouped, "\n", fn {client, client_cells} ->
        "  #{pad(client, 16)}" <> Enum.map_join(rates, "", &step_text(client_cells, &1))
      end)

    header <> "\n" <> rows
  end

  defp step_text(cells, rate) do
    cells
    |> Enum.find(&(&1.rate == rate))
    |> format_step()
    |> pad(22)
  end

  defp format_step(nil), do: "-"
  defp format_step(%{status: "ok"} = cell), do: "p50 #{us(cell.p50)} p99 #{us(cell.p99)}"
  defp format_step(cell), do: cell.status

  defp us(value) when is_number(value) and value >= 9999.9, do: ">10000"
  defp us(value), do: "#{value}"

  defp millions(value) when is_number(value), do: :erlang.float_to_binary(value / 1_000_000, decimals: 2) <> "M"
  defp millions(value), do: "#{value}"

  defp pad(value, width) when is_binary(value), do: String.pad_trailing(value, width)
  defp pad(value, width), do: pad(to_string(value), width)

  defp merge_existing(clients, saturation, stepped, _dir, false), do: {clients, saturation, stepped}

  defp merge_existing(_clients, saturation, stepped, dir, true) do
    fresh = saturation ++ stepped
    fresh_keys = MapSet.new(fresh, &{&1.client, &1.rate})

    merged =
      dir
      |> Path.join("harness.json")
      |> File.read!()
      |> Jason.decode!(keys: :atoms)
      |> Enum.map(&restore_cell/1)
      |> Enum.reject(&MapSet.member?(fresh_keys, {&1.client, &1.rate}))
      |> Kernel.++(fresh)
      |> Enum.sort_by(&client_rank(&1.client))

    {merged_saturation, merged_stepped} = Enum.split_with(merged, &(&1.rate == :max))
    {merged |> Enum.map(& &1.client) |> Enum.uniq(), merged_saturation, merged_stepped}
  end

  defp restore_cell(cell), do: %{cell | client: String.to_atom(cell.client), rate: restore_rate(cell.rate)}

  defp restore_rate("max"), do: :max
  defp restore_rate(rate), do: rate

  defp client_rank(client), do: Enum.find_index(@clients, &(&1 == client)) || length(@clients)

  defp write_json(dir, cells) do
    File.write!(Path.join(dir, "harness.json"), Jason.encode!(cells, pretty: true))
  end

  defp write_markdown(dir, clients, rates, settings, saturation, stepped) do
    content = """
    # Market-data harness

    A synthetic price publisher, a C program on the Aeron C client, runs as its own
    OS process, and each client consumes from it in turn, one client at a time.
    Every cell is run #{settings.repeats} times and the median run is shown: the
    median consume rate flat out, the median p99 at a fixed rate. `harness.json`
    keeps every run's p99 and consume rate (`p99_runs`, `consume_rate_runs`).

    ## Saturation — publisher flat out

    | client | published | consumed | consume rate/s | lag | gaps | p50 µs | p99 µs | p99.9 µs | max µs |
    |---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
    #{Enum.map_join(saturation, "\n", &markdown_saturation_row/1)}

    Latency under saturation is queue depth, not transport cost: the publisher
    outruns the consumer, so each tick waits in the log before it is read. The
    columns that answer "did it keep up" are consume rate and lag.

    ## Rate steps — latency below saturation

    | client | #{Enum.map_join(rates, " | ", &"#{div(&1, 1000)}k/s")} |
    |---|#{Enum.map_join(rates, "|", fn _ -> "---" end)}|
    #{Enum.map_join(clients, "\n", &markdown_step_row(&1, rates, stepped))}

    Each cell is p50 / p99 in µs. A client is not run at a higher rate once it
    fails to consume 98% of what was published at the rate below.


    ### Reading these numbers

    #{saturation_reading(saturation)}
    - Lag is zero everywhere because IPC cannot drop. When a consumer falls behind,
      the publication limit stops the publisher rather than discarding messages, so
      a slow consumer shows up as a lower publish rate and a deeper queue, never as
      loss. A slow consumer's saturation latencies can exceed the histogram's 10 ms
      ceiling for that reason.
    - Latencies are whole microseconds. Every side timestamps with CLOCK_REALTIME,
      which on macOS advances in 1 µs steps, so a p50 of 0.05 µs means the tick was
      read within the same microsecond it was stamped. Clients are separated only
      at 1 µs and above.
    - `java` reads `Instant.now()`, which has the same 1 µs resolution, and adds half
      a step to remove the truncation bias, so where the others report 0.05 µs it
      reports 0.55 µs. The difference is that correction, not Java.

    ## Method

    - Host: Apple M3 Max, 16 logical cores (12 performance + 4 efficiency), macOS, no CPU pinning. Media driver: C `aeronmd_s` at `#{aeron_dir()}`, default threading, shared by every cell.
    - Channel `#{channel()}`, one stream id per cell so a finished cell never collides with the next.
    - The publisher is `bench/harness/price_server.c`. Flat out it offers ticks in batches of 200, one `aeron_publication_offer` per tick; at a fixed rate it publishes whatever the elapsed time says it is behind, at most 200 per step. It stamps each tick with `clock_gettime(CLOCK_REALTIME)` immediately before offering it, and counts the time it spends back-pressured.
    - Each tick is 64 bytes little-endian: u32 instrument id, i64 bid, i64 ask, i64 sequence, i64 publish timestamp, then 28 bytes of padding taken from pool entry `sequence mod 4096` of the 64-byte payload pool every client's benches use (splitmix64, seed 0x5EEDAE20), so every byte of every tick varies. Prices random-walk per instrument across #{System.get_env("HARNESS_INSTRUMENTS", "500")} instruments.
    - Every consumer decodes all five fields of every tick and folds the prices into a running sum, printed to stderr so the decode cannot be optimised away.
    - Each run lasts #{settings.duration} s after the publisher and consumer are connected, and each cell is #{settings.repeats} runs on separate streams. The consumer exits once the stream has been idle for 750 ms.
    - Timestamps are CLOCK_REALTIME on every side: `:os.system_time(:nanosecond)` in Elixir, `clock_gettime(CLOCK_REALTIME)` in C, `time.time_ns()` in Python, `time.Now().UnixNano()` in Go, `SystemTime::now()` in Rust, `clock_gettime(CLOCK_REALTIME)` in .NET, and `Instant.now()` in Java.
    - Latency is recorded in a 100 ns-bucket histogram covering 0–10 ms; percentiles are bucket midpoints. The timestamps themselves are 1 µs steps on macOS (see above).
    - `elixir_direct` polls through `AeronElixir.LogBuffer.Subscriber.collect/2` with a direct image handle.
    """

    File.write!(Path.join(dir, "harness.md"), content)
  end

  defp saturation_reading(saturation) do
    {kept_up, constrained} =
      saturation
      |> Enum.filter(&(&1.status == "ok"))
      |> Enum.sort_by(& &1.consume_rate, :desc)
      |> Enum.split_with(&(&1.back_pressured_ms == 0))

    "- Flat out, a consumer that back-pressures the publisher is reading as fast as it can, so its " <>
      "consume rate is its own maximum. " <> constrained_sentence(constrained) <> kept_up_sentence(kept_up)
  end

  defp constrained_sentence([]), do: ""

  defp constrained_sentence(clients) do
    details =
      Enum.map_join(clients, ", ", fn cell ->
        "`#{cell.client}` #{millions(cell.consume_rate)}/s (publisher back-pressured for #{round(cell.back_pressured_ms)} ms)"
      end)

    "Highest first: #{details}. "
  end

  defp kept_up_sentence([]), do: ""

  defp kept_up_sentence(clients) do
    "#{client_list(clients)} never back-pressured it, so the publisher, not the consumer, set that rate."
  end

  defp client_list(clients), do: Enum.map_join(clients, ", ", &"`#{&1.client}`")

  defp markdown_saturation_row(%{status: "ok"} = cell) do
    "| #{cell.client} | #{millions(cell.published)} | #{millions(cell.consumed)} | #{millions(cell.consume_rate)} | #{millions(cell.lag)} | #{cell.gaps} | #{us(cell.p50)} | #{us(cell.p99)} | #{us(cell.p999)} | #{us(cell.max)} |"
  end

  defp markdown_saturation_row(cell), do: "| #{cell.client} | #{cell.status} | | | | | | | | |"

  defp markdown_step_row(client, rates, stepped) do
    cells = Enum.filter(stepped, &(&1.client == client))
    "| #{client} | " <> Enum.map_join(rates, " | ", fn rate -> markdown_step(cells, rate) end) <> " |"
  end

  defp markdown_step(cells, rate) do
    cells
    |> Enum.find(&(&1.rate == rate))
    |> format_step()
  end

  defp aeron_dir, do: System.get_env("AERON_DIR", "/tmp/ae_drv")

  defp channel,
    do: System.get_env("HARNESS_CHANNEL", "aeron:ipc?alias=prices|term-length=67108864")

  defp aeron_lib_path do
    System.get_env("DYLD_LIBRARY_PATH") || System.get_env("LD_LIBRARY_PATH") ||
      Path.expand("../aeron/build/lib", File.cwd!())
  end

  defp python_interpreter, do: Path.expand("bench/python/.venv/bin/python", File.cwd!())

  defp c_consumer_binary, do: Path.expand("bench/c/build/harness_consumer_c", File.cwd!())

  defp c_price_server_binary, do: Path.expand("bench/c/build/harness_price_server_c", File.cwd!())

  defp java_args do
    jar = Path.expand("~/.m2/repository/io/aeron/aeron-all/1.49.3/aeron-all-1.49.3.jar")
    ["--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED", "-cp", "#{jar}:bench/java/out"]
  end
end

opts =
  System.argv()
  |> Enum.map(fn arg ->
    case String.split(arg, "=", parts: 2) do
      ["clients", value] -> {:clients, value |> String.split(",", trim: true) |> Enum.map(&String.to_atom/1)}
      ["rates", value] -> {:rates, value |> String.split(",") |> Enum.map(&String.to_integer/1)}
      ["duration", value] -> {:duration, String.to_integer(value)}
      ["repeats", value] -> {:repeats, String.to_integer(value)}
      ["merge", "true"] -> {:merge, true}
      _ -> nil
    end
  end)
  |> Enum.reject(&is_nil/1)

Harness.Runner.run(opts)
