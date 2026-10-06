defmodule AeronElixirSite.Client do
  @moduledoc """
  Display identity of each client key in the benchmark results.

  `describe/2` takes the suite a result came from, because one key names different
  code paths in different suites: `elixir` is the public API in the single-process
  suite and direct handles in the multi-worker and request/response suites.
  """

  @enforce_keys [:key, :name, :variant, :family]
  defstruct [:key, :name, :variant, :family]

  @type family :: :elixir | :python | :native | :other
  @type suite :: :single | :workers | :round_trip | :market
  @type t :: %__MODULE__{
          key: String.t(),
          name: String.t(),
          variant: String.t(),
          family: family()
        }

  @shared %{
    "python" => {"pyaeron", "Aeron C client wrapped for CPython", :python},
    "java" => {"Aeron Java", "official Java client", :native},
    "c" => {"Aeron C", "official C client", :native},
    "cpp" => {"Aeron C++", "official C++ wrapper over the C client", :native},
    "go" => {"aergo", "third-party pure Go client", :native},
    "dotnet" => {"Aeron.NET", "Adaptive's .NET port of the Java client", :native},
    "rust" => {"rusteron", "Rust bindings over the Aeron C client", :native}
  }

  @elixir_variants %{
    {:single, "elixir_raw"} => "direct handles, one message per call",
    {:workers, "elixir"} => "direct handles, one message per call",
    {:round_trip, "elixir"} => "direct handles, two VMs",
    {:market, "elixir_direct"} => "direct image handle, Subscriber.collect/2"
  }

  @spec describe(suite(), String.t()) :: t()
  def describe(suite, key),
    do: describe_known(Map.fetch(@shared, key), Map.fetch(@elixir_variants, {suite, key}), key)

  defp describe_known({:ok, {name, variant, family}}, _elixir_variant, key),
    do: %__MODULE__{key: key, name: name, variant: variant, family: family}

  defp describe_known(:error, {:ok, variant}, key),
    do: %__MODULE__{
      key: key,
      name: "aeron_elixir",
      variant: variant,
      family: :elixir
    }

  defp describe_known(:error, :error, key),
    do: %__MODULE__{key: key, name: key, variant: "", family: :other}
end
