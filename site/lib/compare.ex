defmodule AeronElixirSite.Compare do
  @moduledoc """
  Which clients the benchmark charts show. aeron_elixir is always shown; the
  reader chooses the others, starting from pyaeron.

  `clients/1` lists every other client in the results, ordered by name, for the
  reader to choose from. `shown_by_default?/1` says whether a client's rows are
  visible before the reader changes anything, and `width/2` gives a row's bar
  width as a fraction of the largest value among those default rows, so the
  page is drawn to scale before its script runs.
  """

  alias AeronElixirSite.{Chart, Client, Results}

  @default_keys ["python"]

  @spec clients(Results.t()) :: [Client.t()]
  def clients(%Results{} = results) do
    [
      single: results.single,
      workers: results.workers,
      round_trip: results.round_trip,
      market: results.market
    ]
    |> Enum.flat_map(fn {suite, cells} -> Enum.map(cells, &Client.describe(suite, &1.client)) end)
    |> Enum.reject(&(&1.family == :elixir))
    |> Enum.uniq_by(& &1.key)
    |> Enum.sort_by(&String.downcase(&1.name))
  end

  @spec shown_by_default?(Client.t()) :: boolean()
  def shown_by_default?(%Client{family: :elixir}), do: true
  def shown_by_default?(%Client{key: key}), do: key in @default_keys

  @spec width(Chart.t(), Chart.row()) :: float()
  def width(%Chart{rows: rows}, row) do
    largest =
      rows
      |> Enum.filter(&shown_by_default?(&1.client))
      |> Enum.map(& &1.value)
      |> Enum.max(fn -> 1 end)

    min(row.value / largest, 1.0)
  end
end
