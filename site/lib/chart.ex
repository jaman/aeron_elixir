defmodule AeronElixirSite.Chart do
  @moduledoc """
  A ranked bar chart: one row per client, best first, each with a display value,
  detail lines for its tooltip and a bar width as a fraction of the largest value.

  `better` is `:higher` for rates and `:lower` for latencies; it decides the sort
  order and how `versus_python/1` words the comparison.
  """

  alias AeronElixirSite.{Client, Format}

  @enforce_keys [:title, :better, :rows]
  defstruct @enforce_keys

  @type row :: %{
          client: Client.t(),
          value: number(),
          display: String.t(),
          details: [{String.t(), String.t()}],
          width: float()
        }
  @type t :: %__MODULE__{title: String.t(), better: :higher | :lower, rows: [row()]}
  @type entry :: {String.t(), number(), [{String.t(), String.t()}]}

  @spec new(Client.suite(), String.t(), :higher | :lower, (number() -> String.t()), [entry()]) :: t()
  def new(suite, title, better, formatter, entries) do
    largest = entries |> Enum.map(&elem(&1, 1)) |> Enum.max(fn -> 1 end)

    rows =
      entries
      |> Enum.map(fn {key, value, details} ->
        %{
          client: Client.describe(suite, key),
          value: value,
          display: formatter.(value),
          details: details,
          width: value / largest
        }
      end)
      |> Enum.sort_by(& &1.value, sort_order(better))

    %__MODULE__{title: title, better: better, rows: rows}
  end

  @doc """
  The best aeron_elixir row that does the same work as the other clients.
  """
  @spec best_elixir(t()) :: row() | nil
  def best_elixir(%__MODULE__{rows: rows}),
    do: Enum.find(rows, &(&1.client.family == :elixir))

  @spec python(t()) :: row() | nil
  def python(%__MODULE__{rows: rows}), do: Enum.find(rows, &(&1.client.family == :python))

  @doc """
  Compares the best like-for-like aeron_elixir row with pyaeron's, or returns
  `nil` when either is absent from the chart.
  """
  @spec versus_python(t()) :: String.t() | nil
  def versus_python(chart), do: compare(chart.better, best_elixir(chart), python(chart))

  defp sort_order(:higher), do: :desc
  defp sort_order(:lower), do: :asc

  defp compare(_better, nil, _python), do: nil
  defp compare(_better, _elixir, nil), do: nil

  defp compare(better, elixir, python),
    do: "vs pyaeron #{python.display} · #{lead(better, elixir.value, python.value)}"

  defp lead(:higher, ours, theirs) when ours >= theirs, do: "#{Format.ratio(ours / theirs)} higher"
  defp lead(:higher, ours, theirs), do: "pyaeron #{Format.ratio(theirs / ours)} higher"
  defp lead(:lower, ours, theirs) when ours <= theirs, do: "#{Format.ratio(theirs / ours)} lower"
  defp lead(:lower, ours, theirs), do: "pyaeron #{Format.ratio(ours / theirs)} lower"
end
