defmodule AeronElixirSite.Results do
  @moduledoc """
  Benchmark results read from an aeron_elixir checkout's `bench/results` directory.

  `load!/1` reads `summary.json` (single process), every `multiproc-*.json`
  (multi-worker), `rpc.json` (request/response) and `harness.json` (market-data
  harness), keeping only Aeron clients' cells whose status is `"ok"`, plus the
  host description from `rpc.md`. The `beam` baseline (plain `send`/`receive`,
  no Aeron) is not loaded. A missing or malformed file raises.
  """

  @enforce_keys [:single, :workers, :round_trip, :market, :host, :runtime]
  defstruct @enforce_keys

  @non_aeron_clients ["beam"]

  @type single_cell :: %{
          client: String.t(),
          scenario: :latency | :throughput,
          payload: pos_integer(),
          median_us: number() | nil,
          p99_us: number() | nil,
          ops_per_sec: number()
        }
  @type worker_cell :: %{
          client: String.t(),
          payload: pos_integer(),
          workers: pos_integer(),
          ops_per_sec: number()
        }
  @type round_trip_cell :: %{
          client: String.t(),
          pairs: pos_integer(),
          p50_us: number(),
          p99_us: number(),
          p999_us: number(),
          rate: number()
        }
  @type market_cell :: %{
          client: String.t(),
          rate: :saturation | pos_integer(),
          consume_rate: number(),
          p50_us: number(),
          p99_us: number(),
          back_pressured_ms: number()
        }
  @type t :: %__MODULE__{
          single: [single_cell()],
          workers: [worker_cell()],
          round_trip: [round_trip_cell()],
          market: [market_cell()],
          host: String.t(),
          runtime: String.t()
        }

  @spec load!(Path.t()) :: t()
  def load!(dir) do
    summary = read_json!(Path.join(dir, "summary.json"))

    %__MODULE__{
      single: summary |> Map.fetch!("cells") |> ok_cells() |> Enum.map(&single_cell/1),
      workers: dir |> worker_files() |> Enum.flat_map(&cells_of/1) |> ok_cells() |> Enum.map(&worker_cell/1),
      round_trip: dir |> Path.join("rpc.json") |> cells_of() |> ok_cells() |> Enum.map(&round_trip_cell/1),
      market: dir |> Path.join("harness.json") |> read_json!() |> ok_cells() |> Enum.map(&market_cell/1),
      host: host!(Path.join(dir, "rpc.md")),
      runtime: "OTP #{summary["host"]["otp"]} · Elixir #{summary["host"]["beam"]}"
    }
  end

  defp read_json!(path), do: path |> File.read!() |> JSON.decode!()

  defp cells_of(path), do: path |> read_json!() |> Map.fetch!("cells")

  defp worker_files(dir), do: dir |> Path.join("multiproc-*.json") |> Path.wildcard()

  defp ok_cells(cells), do: Enum.filter(cells, &(&1["status"] == "ok" and &1["client"] not in @non_aeron_clients))

  defp single_cell(cell),
    do: %{
      client: cell["client"],
      scenario: String.to_existing_atom(cell["scenario"]),
      payload: cell["payload_size"],
      median_us: cell["median_us"],
      p99_us: cell["p99_us"],
      ops_per_sec: cell["ops_per_sec"]
    }

  defp worker_cell(cell),
    do: %{
      client: cell["client"],
      payload: cell["payload_size"],
      workers: cell["workers"],
      ops_per_sec: cell["ops_per_sec"]
    }

  defp round_trip_cell(cell),
    do: %{
      client: cell["client"],
      pairs: cell["pairs"],
      p50_us: cell["p50_us"],
      p99_us: cell["p99_us"],
      p999_us: cell["p999_us"],
      rate: cell["round_trips_per_sec"]
    }

  defp market_cell(cell),
    do: %{
      client: cell["client"],
      rate: market_rate(cell["rate"]),
      consume_rate: cell["consume_rate"],
      p50_us: cell["p50"],
      p99_us: cell["p99"],
      back_pressured_ms: cell["back_pressured_ms"]
    }

  defp market_rate("max"), do: :saturation
  defp market_rate(rate) when is_integer(rate), do: rate

  defp host!(path) do
    [_line, host] = Regex.run(~r/^- Host: ([^.]+)\./m, File.read!(path))
    host
  end
end
