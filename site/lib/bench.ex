defmodule AeronElixirSite.Bench do
  @moduledoc """
  The benchmark explorer: its tabs, the controls each tab offers, the default
  selection, and the chart for a tab and selection.

  A selection is a map per tab, for example `%{payload: 32, workers: 12}` for
  `:workers`. Controls list their options as `{id, value, label}`, where `id` is
  the option's value as a string.
  """

  alias AeronElixirSite.{Chart, Format, Results}

  @tabs [
    workers: "Many workers",
    round_trip: "Request / response",
    market: "Market data",
    single: "One process"
  ]

  @methods %{
    workers:
      "Each worker owns a private IPC publication and subscription, offers 1000 ticks at a time and polls on back-pressure. aeron_elixir workers are BEAM processes sharing one client; pyaeron runs one OS process and client per worker; Java threads share a client; C and C++ run a client per thread; Go runs one aergo client per goroutine; .NET threads share one Aeron.NET client; Rust runs a client per thread. Every message is distinct: each client draws it from the same 4096-entry pool of varied payloads and stamps a fresh tick into it.",
    round_trip:
      "One 32-byte request in flight: the pinger waits for the echo before sending the next. 1,000,000 measured round trips per pair after 100,000 warmup. Each pair is two OS processes. With N pairs, every pinger waits after its warmup for one shared start time, so all N timed loops run together; the rate is the total round trips divided by the time from the first start to the last finish. aergo needs its caller to run its conductor, so the Go programs call DoWork() while waiting for the first message and once every 1024 operations. Every message is distinct: each client draws it from the same 4096-entry pool of varied payloads and stamps a fresh tick into it.",
    market:
      "A C price publisher runs as its own OS process sending 64-byte ticks; each consumer decodes every tick. Flat out, the publisher outruns every consumer, so the consume rate is each consumer's own maximum; at fixed rates, p99 shows latency below saturation. Each tick's padding comes from the same payload pool, so every byte varies. Every side timestamps with CLOCK_REALTIME, which on macOS moves in 1 µs steps, so fixed-rate latencies are whole microseconds and 0.05 µs means within the same microsecond. Each cell is the median of three runs.",
    single:
      "One publication and one subscription in one process. Latency is the offer-to-poll round trip of one message at a time; throughput offers batches of 1000 and drains. Every message is distinct: each client draws it from the same 4096-entry pool of varied payloads and stamps a fresh tick into it. The tick is stamped inside the timed window, which costs Python the most."
  }

  @type tab :: :workers | :round_trip | :market | :single
  @type option :: {String.t(), term(), String.t()}
  @type control :: %{field: atom(), label: String.t(), options: [option()]}

  @spec tabs() :: [{tab(), String.t()}]
  def tabs, do: @tabs

  @spec method(tab()) :: String.t()
  def method(tab), do: Map.fetch!(@methods, tab)

  @spec defaults(Results.t()) :: %{tab() => map()}
  def defaults(results) do
    %{
      workers: %{payload: smallest(results.workers, :payload), workers: largest(results.workers, :workers)},
      round_trip: %{pairs: smallest(results.round_trip, :pairs), metric: :p50},
      market: %{rate: :saturation},
      single: %{scenario: :latency, payload: smallest(results.single, :payload)}
    }
  end

  @spec controls(Results.t(), tab()) :: [control()]
  def controls(results, :workers),
    do: [
      control(:payload, "Payload", values(results.workers, :payload), &"#{&1} B"),
      control(:workers, "Workers", values(results.workers, :workers), &"N=#{&1}")
    ]

  def controls(results, :round_trip),
    do: [
      control(:metric, "Measure", [:p50, :rate], &metric_label/1),
      control(:pairs, "Pairs", values(results.round_trip, :pairs), &"#{&1}")
    ]

  def controls(results, :market),
    do: [control(:rate, "Publisher", market_rates(results), &rate_label/1)]

  def controls(results, :single),
    do: [
      control(:scenario, "Measure", [:latency, :throughput], &scenario_label/1),
      control(:payload, "Payload", values(results.single, :payload), &"#{&1} B")
    ]

  @doc """
  Returns every selection `controls` can produce: one map per combination of
  their options, keyed by each control's field.
  """
  @spec selections([control()]) :: [map()]
  def selections(controls) do
    Enum.reduce(controls, [%{}], fn control, partial ->
      for selection <- partial, {_id, value, _label} <- control.options, do: Map.put(selection, control.field, value)
    end)
  end

  @doc """
  Names `selection` by its controls' option ids in control order, for example
  `"payload=32&workers=12"`. The page's script builds the same key from the
  pressed buttons to find the chart to show.
  """
  @spec key([control()], map()) :: String.t()
  def key(controls, selection),
    do: Enum.map_join(controls, "&", &"#{&1.field}=#{option_id(Map.fetch!(selection, &1.field))}")

  @spec chart(Results.t(), tab(), map()) :: Chart.t()
  def chart(results, :workers, %{payload: payload, workers: workers}) do
    baseline =
      for cell <- results.workers,
          cell.payload == payload,
          cell.workers == 1,
          into: %{},
          do: {cell.client, cell.ops_per_sec}

    entries =
      for cell <- results.workers, cell.payload == payload, cell.workers == workers do
        {cell.client, cell.ops_per_sec,
         [
           {"Messages/s", Format.rate(cell.ops_per_sec)},
           {"Gain over 1 worker", gain(cell.ops_per_sec, baseline[cell.client])}
         ]}
      end

    Chart.new(
      :workers,
      "Aggregate messages per second · #{workers} workers · #{payload} B",
      :higher,
      &Format.rate/1,
      entries
    )
  end

  def chart(results, :round_trip, %{pairs: pairs, metric: :p50}) do
    Chart.new(
      :round_trip,
      "Median round trip · #{pairs} #{pluralize(pairs, "pair")} · lower is better",
      :lower,
      &Format.micros/1,
      for(cell <- results.round_trip, cell.pairs == pairs, do: {cell.client, cell.p50_us, round_trip_details(cell)})
    )
  end

  def chart(results, :round_trip, %{pairs: pairs, metric: :rate}) do
    Chart.new(
      :round_trip,
      "Round trips per second, all pairs · #{pairs} #{pluralize(pairs, "pair")}",
      :higher,
      &Format.rate/1,
      for(cell <- results.round_trip, cell.pairs == pairs, do: {cell.client, cell.rate, round_trip_details(cell)})
    )
  end

  def chart(results, :market, %{rate: :saturation}) do
    entries =
      for cell <- results.market, cell.rate == :saturation do
        {cell.client, cell.consume_rate,
         [
           {"Consumed/s", Format.precise_rate(cell.consume_rate)},
           {"p50", Format.harness_micros(cell.p50_us)},
           {"p99", Format.harness_micros(cell.p99_us)},
           {"Publisher back-pressured", Format.millis(cell.back_pressured_ms)}
         ]}
      end

    Chart.new(:market, "Ticks consumed per second · publisher flat out", :higher, &Format.precise_rate/1, entries)
  end

  def chart(results, :market, %{rate: rate}) do
    entries =
      for cell <- results.market, cell.rate == rate do
        {cell.client, cell.p99_us,
         [{"p50", Format.harness_micros(cell.p50_us)}, {"p99", Format.harness_micros(cell.p99_us)}]}
      end

    Chart.new(
      :market,
      "p99 tick latency at #{rate_label(rate)} · lower is better",
      :lower,
      &Format.harness_micros/1,
      entries
    )
  end

  def chart(results, :single, %{scenario: :latency, payload: payload}) do
    entries =
      for cell <- results.single, cell.scenario == :latency, cell.payload == payload do
        {cell.client, cell.median_us, [{"Median", Format.micros(cell.median_us)}, {"p99", Format.micros(cell.p99_us)}]}
      end

    Chart.new(
      :single,
      "Median offer-to-poll round trip · #{payload} B · lower is better",
      :lower,
      &Format.micros/1,
      entries
    )
  end

  def chart(results, :single, %{scenario: :throughput, payload: payload}) do
    entries =
      for cell <- results.single, cell.scenario == :throughput, cell.payload == payload do
        {cell.client, cell.ops_per_sec, [{"Messages/s", Format.rate(cell.ops_per_sec)}]}
      end

    Chart.new(:single, "Messages per second, one process · #{payload} B", :higher, &Format.rate/1, entries)
  end

  @doc """
  Clients that ran flat out but have no result at the selected market rate,
  because they did not keep up at a lower rate.
  """
  @spec missing_at_rate(Results.t(), map()) :: [String.t()]
  def missing_at_rate(_results, %{rate: :saturation}), do: []

  def missing_at_rate(results, %{rate: rate}) do
    present = for cell <- results.market, cell.rate == rate, do: cell.client
    for cell <- results.market, cell.rate == :saturation, cell.client not in present, do: cell.client
  end

  defp control(field, label, values, labeler),
    do: %{field: field, label: label, options: Enum.map(values, &{option_id(&1), &1, labeler.(&1)})}

  defp option_id(value) when is_atom(value), do: Atom.to_string(value)
  defp option_id(value) when is_integer(value), do: Integer.to_string(value)

  defp values(cells, key), do: cells |> Enum.map(&Map.fetch!(&1, key)) |> Enum.uniq() |> Enum.sort()

  defp smallest(cells, key), do: cells |> values(key) |> List.first()
  defp largest(cells, key), do: cells |> values(key) |> List.last()

  defp market_rates(results) do
    steps = results.market |> Enum.map(& &1.rate) |> Enum.reject(&(&1 == :saturation)) |> Enum.uniq() |> Enum.sort()
    [:saturation | steps]
  end

  defp metric_label(:p50), do: "Median latency"
  defp metric_label(:rate), do: "Round trips/s"

  defp scenario_label(:latency), do: "Latency"
  defp scenario_label(:throughput), do: "Throughput"

  defp rate_label(:saturation), do: "Flat out"
  defp rate_label(rate), do: "#{Format.round_rate(rate)}/s"

  defp gain(_value, nil), do: "—"
  defp gain(value, baseline), do: Format.ratio(value / baseline)

  defp round_trip_details(cell),
    do: [
      {"p50", Format.micros(cell.p50_us)},
      {"p99", Format.micros(cell.p99_us)},
      {"p99.9", Format.micros(cell.p999_us)},
      {"Round trips/s", Format.rate(cell.rate)}
    ]

  defp pluralize(1, word), do: word
  defp pluralize(_count, word), do: word <> "s"
end
