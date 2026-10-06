defmodule AeronElixir.ChannelUri do
  @moduledoc """
  Builds Aeron channel URIs (`aeron:ipc?…`, `aeron:udp?…`) from keyword
  parameters.

  Parameters are validated against the names Aeron's channel specification
  accepts and rendered in Aeron's `key=value|key=value` query form, with
  underscores in keys turned into dashes (`term_length` → `term-length`).
  Booleans render as `true`/`false`, integers as decimal.

  Accepted parameters: `endpoint`, `interface`, `control`, `control_mode`,
  `term_length`, `mtu`, `initial_term_id`, `term_id`, `term_offset`,
  `session_id`, `alias`, `reliable`, `linger`, `sparse`, `tags`, `ttl`,
  `eos`, `tether`, `group`, `rejoin`, `sndbuf`, `rcvbuf`, `so_sndbuf`,
  `so_rcvbuf`, `receiver_window`, `media_rcv_ts_offset`, `channel_rcv_ts_offset`,
  `channel_snd_ts_offset`, `spies_simulate_connection`, `cc`, `fc`, `gtag`,
  `response_endpoint`, `response_correlation_id`, `nak_delay`, `untethered_window_limit_timeout`,
  `untethered_resting_timeout`, `max_resend`, `stream_id`, `publication_window`.

  `term_length` must be a power of two between 64 KiB and 1 GiB.
  """

  @params ~w(
    endpoint interface control control_mode term_length mtu initial_term_id term_id
    term_offset session_id alias reliable linger sparse tags ttl eos tether group rejoin
    sndbuf rcvbuf so_sndbuf so_rcvbuf receiver_window media_rcv_ts_offset
    channel_rcv_ts_offset channel_snd_ts_offset spies_simulate_connection cc fc gtag
    response_endpoint response_correlation_id nak_delay untethered_window_limit_timeout
    untethered_resting_timeout max_resend stream_id publication_window
  )a

  @min_term_length 64 * 1024
  @max_term_length 1024 * 1024 * 1024

  @type param :: {atom(), String.t() | integer() | boolean()}

  @doc """
  Builds an `aeron:ipc` channel, optionally with parameters.
  """
  @spec ipc([param()]) :: String.t() | {:error, term()}
  def ipc(params \\ []) do
    build("aeron:ipc", params)
  end

  @doc """
  Builds an `aeron:udp` channel. `endpoint` (or `control`) is required.
  """
  @spec udp([param()]) :: String.t() | {:error, term()}
  def udp(params) do
    udp(params, Keyword.has_key?(params, :endpoint) or Keyword.has_key?(params, :control))
  end

  defp udp(params, true), do: build("aeron:udp", params)
  defp udp(_params, false), do: {:error, :endpoint_required}

  @doc """
  Renders `media` (`"aeron:ipc"` or `"aeron:udp"`) with the given parameters.
  """
  @spec build(String.t(), [param()]) :: String.t() | {:error, term()}
  def build(media, params) do
    with :ok <- validate(params), do: render(media, params)
  end

  defp render(media, []), do: media
  defp render(media, params), do: media <> "?" <> Enum.map_join(params, "|", &render_param/1)

  defp validate([]), do: :ok

  defp validate([{key, _value} | _rest]) when key not in @params do
    {:error, {:unknown_param, key}}
  end

  defp validate([{:term_length, length} | rest]) do
    validate_term_length(valid_term_length?(length), length, rest)
  end

  defp validate([_param | rest]), do: validate(rest)

  defp validate_term_length(true, _length, rest), do: validate(rest)
  defp validate_term_length(false, length, _rest), do: {:error, {:invalid_term_length, length}}

  defp valid_term_length?(length)
       when is_integer(length) and length >= @min_term_length and length <= @max_term_length do
    Bitwise.band(length, length - 1) == 0
  end

  defp valid_term_length?(_length), do: false

  defp render_param({key, value}) do
    "#{key |> Atom.to_string() |> String.replace("_", "-")}=#{value}"
  end
end
