Code.require_file("../shared.exs", __DIR__)

alias AeronElixir.Bench.Shared

{:ok, _} = Application.ensure_all_started(:benchee)

defmodule BeamEcho do
  @moduledoc false

  def loop do
    receive do
      {:ping, from, payload} -> send(from, {:pong, payload}); loop()
      :stop -> :ok
    end
  end
end

for size <- Shared.payload_sizes() do
  pool = Shared.payload_pool(size)
  echo = spawn_link(fn -> BeamEcho.loop() end)
  name = "beam_latency_#{size}B"

  suite =
    Benchee.run(
      %{
        name => fn ->
          sequence = Process.get(:bench_sequence, 0)
          Process.put(:bench_sequence, sequence + 1)
          send(echo, {:ping, self(), IO.iodata_to_binary(Shared.message(pool, sequence))})

          receive do
            {:pong, _payload} -> :ok
          end
        end
      },
      warmup: Shared.warmup_seconds(),
      time: Shared.time_seconds(),
      formatters: []
    )

  send(echo, :stop)

  scenario = Enum.find(suite.scenarios, &(&1.job_name == name)) || hd(suite.scenarios)
  stats = scenario.run_time_data.statistics

  ops = stats.ips || 0.0
  median_us = (stats.median || 0) / 1000.0
  mean_us = (stats.average || 0) / 1000.0
  p99_us = (Map.get(stats.percentiles || %{}, 99) || 0) / 1000.0

  IO.puts(
    "{\"client\":\"beam\",\"scenario\":\"latency\"," <>
      "\"payload_size\":#{size}," <>
      "\"ops_per_sec\":#{ops}," <>
      "\"median_us\":#{median_us}," <>
      "\"mean_us\":#{mean_us}," <>
      "\"p99_us\":#{p99_us}}"
  )
end
