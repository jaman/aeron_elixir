defmodule AeronElixir.Protocol.URI do
  @moduledoc """
  Aeron channel URI parsing and validation.

  URIs follow format: aeron:{transport}?{params}
  Transports: udp, ipc
  """

  @type transport :: :udp | :ipc
  @type t :: %__MODULE__{
          transport: transport(),
          endpoint: String.t() | nil,
          interface: String.t() | nil,
          control: String.t() | nil,
          control_mode: :dynamic | :manual,
          tags: [String.t()] | nil,
          alias: String.t() | nil,
          reliable: boolean(),
          ttl: pos_integer() | nil,
          socket_buffer_size: pos_integer() | nil,
          initial_term_id: integer() | nil,
          term_length: pos_integer() | nil,
          term_offset: non_neg_integer() | nil,
          mtu: pos_integer() | nil,
          channel: String.t()
        }

  defstruct transport: nil,
            endpoint: nil,
            interface: nil,
            control: nil,
            control_mode: :dynamic,
            tags: nil,
            alias: nil,
            reliable: true,
            ttl: nil,
            socket_buffer_size: nil,
            initial_term_id: nil,
            term_length: nil,
            term_offset: nil,
            mtu: nil,
            channel: nil

  @spec parse(String.t()) ::
          {:ok, t()} | {:error, :invalid_uri | :invalid_transport | :invalid_params}
  def parse(uri) when is_binary(uri) do
    with ["aeron", rest] <- String.split(uri, ":", parts: 2),
         {:ok, transport_type} <- parse_transport(rest),
         {:ok, params} <- extract_params(rest) do
      uri_struct = struct(__MODULE__, Keyword.put(params, :transport, transport_type))
      {:ok, %{uri_struct | channel: uri}}
    else
      _ -> {:error, :invalid_uri}
    end
  end

  defp parse_transport("udp?" <> _), do: {:ok, :udp}
  defp parse_transport("ipc?" <> _), do: {:ok, :ipc}
  defp parse_transport("udp"), do: {:ok, :udp}
  defp parse_transport("ipc"), do: {:ok, :ipc}
  defp parse_transport(_), do: {:error, :invalid_transport}

  defp extract_params(transport_with_params) do
    transport_with_params
    |> String.split("?", parts: 2)
    |> params_from_segments()
  end

  defp params_from_segments([_transport, ""]), do: {:ok, []}
  defp params_from_segments([_transport, params_string]), do: parse_query_string(params_string)
  defp params_from_segments([_transport]), do: {:ok, []}

  defp parse_query_string(""), do: {:ok, []}

  defp parse_query_string(query_string) when is_binary(query_string) do
    query_string
    |> URI.decode_query()
    |> Enum.reduce_while([], &collect_param/2)
    |> parsed_params()
  end

  defp collect_param({key, value}, acc), do: accumulate_param(parse_param(key, value), acc)

  defp accumulate_param({:ok, parsed}, acc), do: {:cont, [parsed | acc]}
  defp accumulate_param(:error, _acc), do: {:halt, :error}

  defp parsed_params(:error), do: {:error, :invalid_params}
  defp parsed_params(params), do: {:ok, Enum.reverse(params)}

  defp parse_param("endpoint", value), do: {:ok, {:endpoint, value}}
  defp parse_param("interface", value), do: {:ok, {:interface, value}}
  defp parse_param("control", value), do: {:ok, {:control, value}}
  defp parse_param("control-mode", "dynamic"), do: {:ok, {:control_mode, :dynamic}}
  defp parse_param("control-mode", "manual"), do: {:ok, {:control_mode, :manual}}
  defp parse_param("tags", value), do: {:ok, {:tags, String.split(value, ",")}}
  defp parse_param("alias", value), do: {:ok, {:alias, value}}
  defp parse_param("reliable", "true"), do: {:ok, {:reliable, true}}
  defp parse_param("reliable", "false"), do: {:ok, {:reliable, false}}
  defp parse_param("ttl", value), do: parse_integer(:ttl, value)
  defp parse_param("socket-buffer-size", value), do: parse_integer(:socket_buffer_size, value)
  defp parse_param("initial-term-id", value), do: parse_integer(:initial_term_id, value)
  defp parse_param("term-length", value), do: parse_integer(:term_length, value)
  defp parse_param("term-offset", value), do: parse_integer(:term_offset, value)
  defp parse_param("mtu", value), do: parse_integer(:mtu, value)
  defp parse_param(_, _), do: :error

  defp parse_integer(key, value), do: integer_param(key, Integer.parse(value))

  defp integer_param(key, {int, ""}) when int >= 0, do: {:ok, {key, int}}
  defp integer_param(_key, _parsed), do: :error

  @spec to_string(t()) :: String.t()
  def to_string(%__MODULE__{} = uri) do
    uri
    |> build_params()
    |> append_params("aeron:#{uri.transport}")
  end

  defp append_params("", base), do: base
  defp append_params(params, base), do: "#{base}?#{params}"

  defp build_params(%__MODULE__{} = uri) do
    tags_value = tags_value(uri.tags)

    [
      uri_param("endpoint", uri.endpoint, nil, false),
      uri_param("interface", uri.interface, nil, false),
      uri_param("control", uri.control, nil, false),
      uri_param(
        "control-mode",
        Atom.to_string(uri.control_mode),
        uri.control_mode != :dynamic,
        true
      ),
      uri_param("tags", tags_value, uri.tags, true),
      uri_param("alias", uri.alias, nil, false),
      uri_param("reliable", bool_to_string(uri.reliable), not uri.reliable, true),
      uri_param("ttl", maybe_to_string(uri.ttl), uri.ttl, true),
      uri_param(
        "socket-buffer-size",
        maybe_to_string(uri.socket_buffer_size),
        uri.socket_buffer_size,
        true
      ),
      uri_param(
        "initial-term-id",
        maybe_to_string(uri.initial_term_id),
        uri.initial_term_id,
        true
      ),
      uri_param("term-length", maybe_to_string(uri.term_length), uri.term_length, true),
      uri_param("term-offset", maybe_to_string(uri.term_offset), uri.term_offset, true),
      uri_param("mtu", maybe_to_string(uri.mtu), uri.mtu, true)
    ]
    |> Enum.filter(& &1)
    |> Enum.join("&")
  end

  defp tags_value(tags) when is_list(tags), do: Enum.join(tags, ",")
  defp tags_value(_tags), do: nil

  defp maybe_to_string(nil), do: nil
  defp maybe_to_string(value), do: Integer.to_string(value)

  defp uri_param(_key, nil, _include?, _encode?), do: nil
  defp uri_param(_key, _value, false, _encode?), do: nil
  defp uri_param(key, value, true, true), do: "#{key}=#{URI.encode_www_form(value)}"
  defp uri_param(key, value, _include?, false), do: "#{key}=#{value}"

  defp bool_to_string(true), do: "true"
  defp bool_to_string(false), do: "false"
end
