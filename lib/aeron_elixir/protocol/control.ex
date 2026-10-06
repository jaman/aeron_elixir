defmodule AeronElixir.Protocol.Control do
  @moduledoc """
  Aeron control protocol command/response codec for client-driver communication.

  Encoders produce the flyweight body only; the command type id travels in the
  ring-buffer record header, not the payload. Decoders consume the flyweight body
  delivered by the broadcast receiver alongside its message type id. Byte layouts
  match the base command flyweights exactly:

  * `PublicationMessageFlyweight` / `SubscriptionMessageFlyweight`
  * `CorrelatedMessageFlyweight`
  * `PublicationBuffersReadyFlyweight` / `SubscriptionReadyFlyweight`
  * `ImageBuffersReadyFlyweight` / `ErrorResponseFlyweight`

  All fields are little-endian. ASCII fields are an `i32` length prefix followed by
  the bytes, mirroring `MutableDirectBuffer.putStringAscii`.
  """

  @command_add_publication 0x01
  @command_remove_publication 0x02
  @command_add_exclusive_publication 0x03
  @command_add_subscription 0x04
  @command_remove_subscription 0x05
  @command_client_keepalive 0x06
  @command_add_destination 0x07
  @command_remove_destination 0x08
  @command_add_counter 0x09
  @command_remove_counter 0x0A
  @command_client_close 0x0B
  @command_add_rcv_destination 0x0C
  @command_remove_rcv_destination 0x0D

  @response_on_error 0x0F01
  @response_on_available_image 0x0F02
  @response_on_publication_ready 0x0F03
  @response_on_operation_success 0x0F04
  @response_on_unavailable_image 0x0F05
  @response_on_exclusive_publication_ready 0x0F06
  @response_on_subscription_ready 0x0F07
  @response_on_counter_ready 0x0F08
  @response_on_unavailable_counter 0x0F09
  @response_on_client_timeout 0x0F0A

  @registration_id_new -1

  @type command ::
          :add_publication
          | :remove_publication
          | :add_exclusive_publication
          | :add_subscription
          | :remove_subscription
          | :client_keepalive
          | :add_counter
          | :remove_counter
          | :client_close

  @type response ::
          :on_error
          | :on_available_image
          | :on_publication_ready
          | :on_operation_success
          | :on_unavailable_image
          | :on_exclusive_publication_ready
          | :on_subscription_ready
          | :on_counter_ready
          | :on_unavailable_counter
          | :on_client_timeout

  @doc """
  Type id of the ADD_PUBLICATION command.
  """
  @spec command_add_publication() :: 0x01
  def command_add_publication, do: @command_add_publication

  @doc """
  Type id of the ADD_EXCLUSIVE_PUBLICATION command.
  """
  @spec command_add_exclusive_publication() :: 0x03
  def command_add_exclusive_publication, do: @command_add_exclusive_publication

  @doc """
  Type id of the ADD_SUBSCRIPTION command.
  """
  @spec command_add_subscription() :: 0x04
  def command_add_subscription, do: @command_add_subscription

  @doc """
  Type id of the CLIENT_KEEPALIVE command.
  """
  @spec command_client_keepalive() :: 0x06
  def command_client_keepalive, do: @command_client_keepalive

  @doc """
  Type id of the CLIENT_CLOSE command.
  """
  @spec command_client_close() :: 0x0B
  def command_client_close, do: @command_client_close

  @doc """
  Type id of the REMOVE_PUBLICATION command.
  """
  @spec command_remove_publication() :: 0x02
  def command_remove_publication, do: @command_remove_publication

  @doc """
  Type id of the REMOVE_SUBSCRIPTION command.
  """
  @spec command_remove_subscription() :: 0x05
  def command_remove_subscription, do: @command_remove_subscription

  @doc """
  Type id of the ADD_DESTINATION command, which adds a destination to a
  publication on a channel in manual control mode.
  """
  @spec command_add_destination() :: 0x07
  def command_add_destination, do: @command_add_destination

  @doc """
  Type id of the REMOVE_DESTINATION command.
  """
  @spec command_remove_destination() :: 0x08
  def command_remove_destination, do: @command_remove_destination

  @doc """
  Type id of the ADD_RCV_DESTINATION command, which adds a destination to a
  subscription on a channel in manual control mode.
  """
  @spec command_add_rcv_destination() :: 0x0C
  def command_add_rcv_destination, do: @command_add_rcv_destination

  @doc """
  Type id of the REMOVE_RCV_DESTINATION command.
  """
  @spec command_remove_rcv_destination() :: 0x0D
  def command_remove_rcv_destination, do: @command_remove_rcv_destination

  @doc """
  Type id of the ADD_COUNTER command.
  """
  @spec command_add_counter() :: 0x09
  def command_add_counter, do: @command_add_counter

  @doc """
  Type id of the REMOVE_COUNTER command.
  """
  @spec command_remove_counter() :: 0x0A
  def command_remove_counter, do: @command_remove_counter

  @doc """
  Type id of the ON_ERROR response.
  """
  @spec response_on_error() :: 0x0F01
  def response_on_error, do: @response_on_error

  @doc """
  Type id of the ON_UNAVAILABLE_IMAGE response.
  """
  @spec response_on_unavailable_image() :: 0x0F05
  def response_on_unavailable_image, do: @response_on_unavailable_image

  @doc """
  Type id of the ON_AVAILABLE_IMAGE response.
  """
  @spec response_on_available_image() :: 0x0F02
  def response_on_available_image, do: @response_on_available_image

  @doc """
  Type id of the ON_PUBLICATION_READY response.
  """
  @spec response_on_publication_ready() :: 0x0F03
  def response_on_publication_ready, do: @response_on_publication_ready

  @doc """
  Type id of the ON_OPERATION_SUCCESS response.
  """
  @spec response_on_operation_success() :: 0x0F04
  def response_on_operation_success, do: @response_on_operation_success

  @doc """
  Type id of the ON_SUBSCRIPTION_READY response.
  """
  @spec response_on_subscription_ready() :: 0x0F07
  def response_on_subscription_ready, do: @response_on_subscription_ready

  @doc """
  Type id of the ON_COUNTER_READY response.
  """
  @spec response_on_counter_ready() :: 0x0F08
  def response_on_counter_ready, do: @response_on_counter_ready

  @doc """
  Type id of the ON_UNAVAILABLE_COUNTER response.
  """
  @spec response_on_unavailable_counter() :: 0x0F09
  def response_on_unavailable_counter, do: @response_on_unavailable_counter

  @doc """
  The registration id that requests a new subscription registration.
  """
  @spec registration_id_new() :: -1
  def registration_id_new, do: @registration_id_new

  @doc """
  Encodes an ADD_PUBLICATION command (PublicationMessageFlyweight layout).
  """
  @spec encode_add_publication(integer(), integer(), integer(), String.t()) :: binary()
  def encode_add_publication(client_id, correlation_id, stream_id, channel)
      when is_integer(client_id) and is_integer(correlation_id) and is_integer(stream_id) and
             is_binary(channel) do
    <<client_id::little-signed-64, correlation_id::little-signed-64, stream_id::little-signed-32>> <>
      encode_ascii(channel)
  end

  @doc """
  Encodes an ADD_EXCLUSIVE_PUBLICATION request; the body layout is the same as
  `encode_add_publication/4`, only the command type id differs.
  """
  @spec encode_add_exclusive_publication(integer(), integer(), integer(), String.t()) :: binary()
  def encode_add_exclusive_publication(client_id, correlation_id, stream_id, channel) do
    encode_add_publication(client_id, correlation_id, stream_id, channel)
  end

  @doc """
  Encodes an ADD_SUBSCRIPTION command (SubscriptionMessageFlyweight layout).
  """
  @spec encode_add_subscription(integer(), integer(), integer(), integer(), String.t()) ::
          binary()
  def encode_add_subscription(
        client_id,
        correlation_id,
        registration_correlation_id,
        stream_id,
        channel
      )
      when is_integer(client_id) and is_integer(correlation_id) and
             is_integer(registration_correlation_id) and is_integer(stream_id) and
             is_binary(channel) do
    <<client_id::little-signed-64, correlation_id::little-signed-64,
      registration_correlation_id::little-signed-64, stream_id::little-signed-32>> <>
      encode_ascii(channel)
  end

  @doc """
  Encodes an ADD_COUNTER command (CounterMessageFlyweight layout).
  """
  @spec encode_add_counter(integer(), integer(), integer(), binary(), String.t()) :: binary()
  def encode_add_counter(client_id, correlation_id, type_id, key, label)
      when is_integer(client_id) and is_integer(correlation_id) and is_integer(type_id) and
             is_binary(key) and is_binary(label) do
    <<client_id::little-signed-64, correlation_id::little-signed-64, type_id::little-signed-32>> <>
      encode_key(key) <> encode_ascii(label)
  end

  @doc """
  Encodes a REMOVE_COUNTER command (RemoveMessageFlyweight layout).
  """
  @spec encode_remove_counter(integer(), integer(), integer()) :: binary()
  def encode_remove_counter(client_id, correlation_id, registration_id)
      when is_integer(client_id) and is_integer(correlation_id) and is_integer(registration_id) do
    <<client_id::little-signed-64, correlation_id::little-signed-64,
      registration_id::little-signed-64>>
  end

  @doc """
  Encodes a RemoveMessageFlyweight body (REMOVE_PUBLICATION and
  REMOVE_SUBSCRIPTION): client id, correlation id and the registration id of the
  resource to remove.
  """
  @spec encode_remove(integer(), integer(), integer()) :: binary()
  def encode_remove(client_id, correlation_id, registration_id)
      when is_integer(client_id) and is_integer(correlation_id) and is_integer(registration_id) do
    <<client_id::little-signed-64, correlation_id::little-signed-64,
      registration_id::little-signed-64>>
  end

  @doc """
  Encodes a CLIENT_KEEPALIVE command (CorrelatedMessageFlyweight layout).
  """
  @spec encode_client_keepalive(integer(), integer()) :: binary()
  def encode_client_keepalive(client_id, correlation_id)
      when is_integer(client_id) and is_integer(correlation_id) do
    encode_correlated(client_id, correlation_id)
  end

  @doc """
  Encodes a CLIENT_CLOSE command (CorrelatedMessageFlyweight layout).
  """
  @spec encode_client_close(integer(), integer()) :: binary()
  def encode_client_close(client_id, correlation_id)
      when is_integer(client_id) and is_integer(correlation_id) do
    encode_correlated(client_id, correlation_id)
  end

  @doc """
  Decodes a to-clients record by its `type_id` into `{response_tag, fields}`.
  """
  @spec decode(integer(), binary()) :: {:ok, {response(), map()}} | {:error, term()}
  def decode(@response_on_publication_ready, body) do
    with {:ok, decoded} <- decode_publication_ready(body),
         do: {:ok, {:on_publication_ready, decoded}}
  end

  def decode(@response_on_exclusive_publication_ready, body) do
    with {:ok, decoded} <- decode_publication_ready(body),
         do: {:ok, {:on_exclusive_publication_ready, decoded}}
  end

  def decode(@response_on_subscription_ready, body) do
    with {:ok, decoded} <- decode_subscription_ready(body),
         do: {:ok, {:on_subscription_ready, decoded}}
  end

  def decode(@response_on_available_image, body) do
    with {:ok, decoded} <- decode_available_image(body), do: {:ok, {:on_available_image, decoded}}
  end

  def decode(@response_on_unavailable_image, body) do
    with {:ok, decoded} <- decode_image_message(body),
         do: {:ok, {:on_unavailable_image, decoded}}
  end

  def decode(@response_on_error, body) do
    with {:ok, decoded} <- decode_error(body), do: {:ok, {:on_error, decoded}}
  end

  def decode(@response_on_operation_success, body) do
    with {:ok, decoded} <- decode_operation_success(body),
         do: {:ok, {:on_operation_success, decoded}}
  end

  def decode(@response_on_counter_ready, body) do
    with {:ok, decoded} <- decode_counter_update(body), do: {:ok, {:on_counter_ready, decoded}}
  end

  def decode(@response_on_unavailable_counter, body) do
    with {:ok, decoded} <- decode_counter_update(body),
         do: {:ok, {:on_unavailable_counter, decoded}}
  end

  def decode(@response_on_client_timeout, <<client_id::little-signed-64, _rest::binary>>),
    do: {:ok, {:on_client_timeout, %{client_id: client_id}}}

  def decode(@response_on_client_timeout, _body), do: {:error, :truncated_client_timeout}

  def decode(type_id, _body) when is_integer(type_id),
    do: {:error, {:unknown_response_type, type_id}}

  @doc """
  Decodes an ON_PUBLICATION_READY body (PublicationBuffersReadyFlyweight layout).
  """
  @spec decode_publication_ready(binary()) :: {:ok, map()} | {:error, term()}
  def decode_publication_ready(
        <<correlation_id::little-signed-64, registration_id::little-signed-64,
          session_id::little-signed-32, stream_id::little-signed-32,
          publication_limit_counter_id::little-signed-32,
          channel_status_indicator_id::little-signed-32, log_file_name_length::little-signed-32,
          rest::binary>>
      )
      when log_file_name_length >= 0 and byte_size(rest) >= log_file_name_length do
    <<log_file_name::binary-size(^log_file_name_length), _padding::binary>> = rest

    {:ok,
     %{
       correlation_id: correlation_id,
       registration_id: registration_id,
       session_id: session_id,
       stream_id: stream_id,
       publication_limit_counter_id: publication_limit_counter_id,
       channel_status_indicator_id: channel_status_indicator_id,
       log_file_name: log_file_name
     }}
  end

  def decode_publication_ready(_), do: {:error, :invalid_publication_ready}

  @doc """
  Decodes an ON_SUBSCRIPTION_READY body (SubscriptionReadyFlyweight layout).
  """
  @spec decode_subscription_ready(binary()) :: {:ok, map()} | {:error, term()}
  def decode_subscription_ready(
        <<correlation_id::little-signed-64, channel_status_indicator_id::little-signed-32,
          _rest::binary>>
      ) do
    {:ok,
     %{
       correlation_id: correlation_id,
       channel_status_indicator_id: channel_status_indicator_id
     }}
  end

  def decode_subscription_ready(_), do: {:error, :invalid_subscription_ready}

  @doc """
  Decodes an ON_AVAILABLE_IMAGE body (ImageBuffersReadyFlyweight layout).
  """
  @spec decode_available_image(binary()) :: {:ok, map()} | {:error, term()}
  def decode_available_image(
        <<correlation_id::little-signed-64, session_id::little-signed-32,
          stream_id::little-signed-32, subscription_registration_id::little-signed-64,
          subscriber_position_id::little-signed-32, log_file_name_length::little-signed-32,
          rest::binary>>
      )
      when log_file_name_length >= 0 and byte_size(rest) >= log_file_name_length do
    <<log_file_name::binary-size(^log_file_name_length), after_log::binary>> = rest

    with {:ok, source_identity} <- decode_aligned_ascii(after_log, log_file_name_length) do
      {:ok,
       %{
         correlation_id: correlation_id,
         session_id: session_id,
         stream_id: stream_id,
         subscription_registration_id: subscription_registration_id,
         subscriber_position_id: subscriber_position_id,
         log_file_name: log_file_name,
         source_identity: source_identity
       }}
    end
  end

  def decode_available_image(_), do: {:error, :invalid_available_image}

  @doc """
  Decodes an ON_ERROR body (ErrorResponseFlyweight layout).
  """
  @spec decode_error(binary()) :: {:ok, map()} | {:error, term()}
  def decode_error(
        <<offending_command_correlation_id::little-signed-64, error_code::little-signed-32,
          error_message_length::little-signed-32, rest::binary>>
      )
      when error_message_length >= 0 and byte_size(rest) >= error_message_length do
    <<error_message::binary-size(^error_message_length), _padding::binary>> = rest

    {:ok,
     %{
       offending_command_correlation_id: offending_command_correlation_id,
       error_code: error_code,
       error_message: error_message
     }}
  end

  def decode_error(_), do: {:error, :invalid_error_response}

  @doc """
  Decodes an ImageMessageFlyweight body (ON_UNAVAILABLE_IMAGE): correlation id,
  subscription registration id, stream id and channel.
  """
  @spec decode_image_message(binary()) :: {:ok, map()} | {:error, :invalid_image_message}
  def decode_image_message(
        <<correlation_id::little-signed-64, subscription_registration_id::little-signed-64,
          stream_id::little-signed-32, channel_length::little-signed-32, rest::binary>>
      )
      when channel_length >= 0 and byte_size(rest) >= channel_length do
    <<channel::binary-size(^channel_length), _padding::binary>> = rest

    {:ok,
     %{
       correlation_id: correlation_id,
       subscription_registration_id: subscription_registration_id,
       stream_id: stream_id,
       channel: channel
     }}
  end

  def decode_image_message(_), do: {:error, :invalid_image_message}

  @doc """
  Decodes an ON_OPERATION_SUCCESS body (OperationSucceededFlyweight layout).
  """
  @spec decode_operation_success(binary()) :: {:ok, map()} | {:error, term()}
  def decode_operation_success(<<correlation_id::little-signed-64, _rest::binary>>) do
    {:ok, %{correlation_id: correlation_id}}
  end

  def decode_operation_success(_), do: {:error, :invalid_operation_success}

  @doc """
  Decodes an ON_COUNTER_READY or ON_UNAVAILABLE_COUNTER body (CounterUpdateFlyweight layout).
  """
  @spec decode_counter_update(binary()) :: {:ok, map()} | {:error, term()}
  def decode_counter_update(
        <<correlation_id::little-signed-64, counter_id::little-signed-32, _rest::binary>>
      ) do
    {:ok, %{correlation_id: correlation_id, counter_id: counter_id}}
  end

  def decode_counter_update(_), do: {:error, :invalid_counter_update}

  defp encode_correlated(client_id, correlation_id) do
    <<client_id::little-signed-64, correlation_id::little-signed-64>>
  end

  @doc """
  Encodes a destination command body.

  `registration_correlation_id` is the registration id of the publication or
  subscription the destination belongs to, and `channel` is the destination
  endpoint URI. The same body serves ADD_DESTINATION, REMOVE_DESTINATION,
  ADD_RCV_DESTINATION and REMOVE_RCV_DESTINATION; the command type id selects
  which.
  """
  @spec encode_destination(integer(), integer(), integer(), String.t()) :: binary()
  def encode_destination(client_id, correlation_id, registration_correlation_id, channel)
      when is_integer(client_id) and is_integer(correlation_id) and
             is_integer(registration_correlation_id) and is_binary(channel) do
    <<client_id::little-signed-64, correlation_id::little-signed-64,
      registration_correlation_id::little-signed-64>> <> encode_ascii(channel)
  end

  defp encode_ascii(value) do
    <<byte_size(value)::little-signed-32>> <> value
  end

  defp encode_key(key) do
    padding = padding_to_align(byte_size(key), 4)
    <<byte_size(key)::little-signed-32>> <> key <> <<0::size(padding)-unit(8)>>
  end

  defp decode_aligned_ascii(buffer, log_file_name_length) do
    skip_padding(buffer, padding_to_align(log_file_name_length, 4))
  end

  defp skip_padding(buffer, aligned_padding) when byte_size(buffer) >= aligned_padding do
    decode_source_identity(
      binary_part(buffer, aligned_padding, byte_size(buffer) - aligned_padding)
    )
  end

  defp skip_padding(_buffer, _aligned_padding), do: {:error, :invalid_available_image}

  defp decode_source_identity(<<source_length::little-signed-32, rest::binary>>)
       when source_length >= 0 and byte_size(rest) >= source_length do
    <<source_identity::binary-size(^source_length), _trailing::binary>> = rest
    {:ok, source_identity}
  end

  defp decode_source_identity(_buffer), do: {:error, :invalid_available_image}

  defp padding_to_align(length, alignment) do
    rem(alignment - rem(length, alignment), alignment)
  end
end
