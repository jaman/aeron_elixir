defmodule AeronElixirSite.Format do
  @moduledoc """
  Number formatting for rates, microsecond latencies and ratios.
  """

  @harness_ceiling_us 9_999.0

  @spec rate(number()) :: String.t()
  def rate(value) when value >= 1.0e6, do: fixed(value / 1.0e6, 2) <> "M"
  def rate(value) when value >= 1.0e3, do: fixed(value / 1.0e3, 1) <> "k"
  def rate(value), do: fixed(value, 0)

  @doc """
  Formats a rate with three decimals in millions, for rows whose rates differ by
  less than the two-decimal format shows.
  """
  @spec precise_rate(number()) :: String.t()
  def precise_rate(value) when value >= 1.0e6, do: fixed(value / 1.0e6, 3) <> "M"
  def precise_rate(value), do: rate(value)

  @doc """
  Formats a whole-number rate for a label: `250_000` is `"250k"`, `2_000_000` is `"2M"`.
  """
  @spec round_rate(pos_integer()) :: String.t()
  def round_rate(value) when rem(value, 1_000_000) == 0, do: "#{div(value, 1_000_000)}M"
  def round_rate(value) when rem(value, 1_000) == 0, do: "#{div(value, 1_000)}k"
  def round_rate(value), do: Integer.to_string(value)

  @spec micros(number()) :: String.t()
  def micros(value) when value < 1, do: fixed(value, 3) <> " µs"
  def micros(value) when value < 10, do: fixed(value, 2) <> " µs"
  def micros(value) when value < 1_000, do: fixed(value, 1) <> " µs"
  def micros(value), do: fixed(value / 1_000, 2) <> " ms"

  @doc """
  Formats a market-harness latency, whose histogram tops out at 10 ms.
  """
  @spec harness_micros(number()) :: String.t()
  def harness_micros(value) when value >= @harness_ceiling_us, do: "> 10 ms"
  def harness_micros(value), do: micros(value)

  @spec millis(number()) :: String.t()
  def millis(value), do: fixed(value, 0) <> " ms"

  @spec ratio(number()) :: String.t()
  def ratio(value), do: fixed(value, 1) <> "×"

  defp fixed(value, decimals), do: :erlang.float_to_binary(value * 1.0, decimals: decimals)
end
