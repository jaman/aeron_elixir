defmodule AeronElixirSite.Explorer do
  @moduledoc """
  Everything the static benchmark explorer shows, computed ahead of time.

  `tabs/1` returns one entry per tab, in tab order, with its controls, its
  default selection, its method note and every chart its controls can select.
  Each chart carries the key `Bench.key/2` gives its selection and whether it is
  the one shown before any button is pressed.
  """

  alias AeronElixirSite.{Bench, Chart, Results}

  @type chart_view :: %{key: String.t(), chart: Chart.t(), missing: [String.t()], shown?: boolean()}
  @type tab_view :: %{
          id: Bench.tab(),
          label: String.t(),
          method: String.t(),
          controls: [Bench.control()],
          default: map(),
          charts: [chart_view()]
        }

  @first_tab :workers

  @spec tabs(Results.t()) :: [tab_view()]
  def tabs(results) do
    defaults = Bench.defaults(results)

    for {tab, label} <- Bench.tabs() do
      controls = Bench.controls(results, tab)
      default = Map.fetch!(defaults, tab)

      %{
        id: tab,
        label: label,
        method: Bench.method(tab),
        controls: controls,
        default: default,
        charts: Enum.map(Bench.selections(controls), &chart_view(results, tab, controls, &1, default))
      }
    end
  end

  @spec first_tab() :: Bench.tab()
  def first_tab, do: @first_tab

  defp chart_view(results, tab, controls, selection, default),
    do: %{
      key: Bench.key(controls, selection),
      chart: Bench.chart(results, tab, selection),
      missing: missing(results, tab, selection),
      shown?: selection == default
    }

  defp missing(results, :market, selection), do: Bench.missing_at_rate(results, selection)
  defp missing(_results, _tab, _selection), do: []
end
