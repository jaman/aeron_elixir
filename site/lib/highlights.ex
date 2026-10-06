defmodule AeronElixirSite.Highlights do
  @moduledoc """
  The headline figures shown at the top of the page, each computed from the
  benchmark results: a value, what it measures, and how pyaeron compares.
  """

  alias AeronElixirSite.{Bench, Chart, Client, Format, Results}

  @type tile :: %{value: String.t(), label: String.t(), detail: String.t()}

  @spec all(Results.t()) :: [tile()]
  def all(results) do
    defaults = Bench.defaults(results)
    most_pairs = results.round_trip |> Enum.map(& &1.pairs) |> Enum.max()
    most_workers = defaults.workers.workers

    [
      chart_tile(
        Bench.chart(results, :round_trip, %{pairs: 1, metric: :p50}),
        "Median request/response over IPC, one pair"
      ),
      chart_tile(
        Bench.chart(results, :round_trip, %{pairs: most_pairs, metric: :rate}),
        "Round trips per second across #{most_pairs} pairs"
      ),
      chart_tile(
        Bench.chart(results, :workers, defaults.workers),
        "Messages per second, #{most_workers} workers, #{defaults.workers.payload} B"
      ),
      scaling_tile(results, defaults.workers.payload, most_workers)
    ]
  end

  defp chart_tile(chart, label) do
    best = Chart.best_elixir(chart)
    %{value: best.display, label: label, detail: Chart.versus_python(chart) || best.client.variant}
  end

  defp scaling_tile(results, payload, most_workers) do
    ranked =
      results.workers
      |> Enum.filter(&(&1.payload == payload))
      |> Enum.group_by(& &1.client)
      |> Enum.map(fn {client, cells} -> {client, gain(cells, most_workers)} end)
      |> Enum.sort_by(&elem(&1, 1), :desc)

    {client, best_gain} = Enum.find(ranked, fn {client, _gain} -> like_for_like_elixir?(client) end)
    rank = Enum.find_index(ranked, &(elem(&1, 0) == client)) + 1

    %{
      value: Format.ratio(best_gain),
      label: "Throughput gain from 1 to #{most_workers} workers, #{payload} B",
      detail: "#{ordinal(rank)} of #{length(ranked)} clients"
    }
  end

  defp gain(cells, most_workers) do
    by_workers = Map.new(cells, &{&1.workers, &1.ops_per_sec})
    Map.fetch!(by_workers, most_workers) / Map.fetch!(by_workers, 1)
  end

  defp like_for_like_elixir?(key) do
    client = Client.describe(:workers, key)
    client.family == :elixir
  end

  defp ordinal(1), do: "Highest"
  defp ordinal(rank), do: "Rank #{rank}"
end
