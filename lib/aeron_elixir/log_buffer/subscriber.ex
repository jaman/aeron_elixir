defmodule AeronElixir.LogBuffer.Subscriber do
  @moduledoc """
  Runtime image polling over a mapped log buffer.

  Holds the geometry the driver assigned to an image (the log base address, term
  length, session id, position geometry and the subscriber-position counter
  address) and reads data frames from the active term partition via the NIF.

  `collect/2` and `poll/3` read from the subscriber position, reassemble
  fragmented messages, and advance the subscriber position past every message
  returned, released so the driver can reclaim space. A message whose fragments
  are not all published yet is left for a later call. The struct is immutable and
  valid from any process, but only one process may poll a given image at a time.

  `region` keeps the log mapping alive, and the log mapping keeps the CnC mapping
  that holds the subscriber-position counter alive, so every address in a handle
  stays valid for as long as the handle is reachable.
  """

  alias AeronElixir.Header
  alias AeronElixir.LogBuffer.Mapping
  alias AeronElixir.NIF

  @log_meta_data_length 4096

  @type t :: %__MODULE__{
          correlation_id: integer(),
          subscription_registration_id: integer(),
          log_base_address: integer(),
          metadata_address: integer(),
          term_length: pos_integer(),
          session_id: integer(),
          initial_term_id: integer(),
          position_bits_to_shift: pos_integer(),
          subscriber_position_address: integer(),
          closed_address: integer(),
          source_identity: String.t() | nil,
          geometry: binary(),
          region: reference()
        }

  @type poll_action :: :continue | :break | :abort | :commit

  defstruct [
    :correlation_id,
    :subscription_registration_id,
    :log_base_address,
    :metadata_address,
    :term_length,
    :session_id,
    :initial_term_id,
    :position_bits_to_shift,
    :subscriber_position_address,
    :closed_address,
    :source_identity,
    :geometry,
    :region
  ]

  @log_end_of_stream_position_offset 128

  @doc """
  Builds the handle for a mapped image log from the driver's response fields and the log descriptor.
  """
  @spec new(
          integer(),
          integer(),
          Mapping.t(),
          integer(),
          struct(),
          integer(),
          String.t() | nil
        ) :: t()
  def new(
        correlation_id,
        subscription_registration_id,
        %Mapping{} = mapping,
        session_id,
        descriptor,
        subscriber_position_address,
        source_identity \\ nil
      ) do
    image = %__MODULE__{
      region: mapping.region,
      correlation_id: correlation_id,
      subscription_registration_id: subscription_registration_id,
      log_base_address: mapping.base_address,
      metadata_address: mapping.base_address + (mapping.length - @log_meta_data_length),
      term_length: descriptor.term_buffer_length,
      session_id: session_id,
      initial_term_id: descriptor.initial_term_id,
      position_bits_to_shift: descriptor.position_bits_to_shift,
      subscriber_position_address: subscriber_position_address,
      closed_address: NIF.closed_flag_address(mapping.region),
      source_identity: source_identity
    }

    %{image | geometry: encode_geometry(image)}
  end

  defp encode_geometry(image) do
    <<
      image.log_base_address::native-signed-64,
      image.metadata_address::native-signed-64,
      image.term_length::native-signed-64,
      image.initial_term_id::native-signed-64,
      image.position_bits_to_shift::native-signed-64,
      image.subscriber_position_address::native-signed-64,
      image.closed_address::native-signed-64
    >>
  end

  @doc """
  Returns whether the image has been closed, either because its subscription was
  closed or because the client lost its media driver. A closed image reads
  nothing.
  """
  @spec closed?(t()) :: boolean()
  def closed?(%__MODULE__{closed_address: address}) do
    {:ok, flag} = NIF.read_int32(address)
    flag != 0
  end

  @doc """
  Reads up to `fragment_limit` whole messages from the image in one NIF call and
  returns `{count, payloads}`, advancing the subscriber position past them.
  """
  @spec collect(t(), pos_integer()) :: {non_neg_integer(), [binary()]}
  def collect(%__MODULE__{geometry: geometry}, fragment_limit)
      when is_integer(fragment_limit) and fragment_limit > 0 do
    NIF.poll_collect(geometry, fragment_limit)
  end

  @doc """
  Reads the next whole message and returns its payload, or `nil` when no
  complete message is waiting or the image is closed.

  The payload is a binary of its own, shared with nothing. This is the cheapest
  way to read one message at a time; `collect/2` reads many in one call.
  """
  @spec next(t()) :: binary() | nil
  def next(%__MODULE__{geometry: geometry}), do: NIF.poll_next(geometry)

  @doc """
  Reads up to `fragment_limit` whole messages from the image and calls
  `handler.(payload, header)` for each, in stream order.

  `header` is an encoded `AeronElixir.Header`; read its fields with the
  `AeronElixir.Header` functions or a binary match. Returns the number of
  messages delivered. Only one process may poll a given image.
  """
  @spec poll(t(), pos_integer(), (binary(), Header.t() -> term())) :: non_neg_integer()
  def poll(%__MODULE__{geometry: geometry}, fragment_limit, handler)
      when is_integer(fragment_limit) and fragment_limit > 0 and is_function(handler, 2) do
    {count, framed} = NIF.poll_collect_framed(geometry, fragment_limit)
    Enum.each(framed, fn {header, payload} -> handler.(payload, header) end)
    count
  end

  @doc """
  Polls like `poll/3` but lets the handler steer consumption with its return
  value: `:continue` consumes the message, `:commit` consumes it and releases the
  subscriber position at once, `:break` consumes it and stops the poll, `:abort`
  stops without consuming it so the next poll delivers it again. Returns the
  number of messages consumed.
  """
  @spec controlled_poll(t(), pos_integer(), (binary(), Header.t() -> poll_action())) ::
          non_neg_integer()
  def controlled_poll(%__MODULE__{} = image, fragment_limit, handler)
      when is_integer(fragment_limit) and fragment_limit > 0 and is_function(handler, 2) do
    {_count, framed} = NIF.peek_collect_framed(image.geometry, fragment_limit)
    start_position = position(image)
    {consumed, committed} = deliver_controlled(image, framed, handler, start_position, 0)
    commit_position(image, start_position, committed)
    consumed
  end

  defp commit_position(_image, position, position), do: :ok

  defp commit_position(image, _start_position, committed),
    do: NIF.write_int64_ordered(image.subscriber_position_address, committed)

  @doc """
  Returns the image's subscriber position: the stream position of the next
  message a poll would deliver.
  """
  @spec position(t()) :: integer()
  def position(%__MODULE__{subscriber_position_address: address}) do
    {:ok, position} = NIF.atomic_get_int64(address)
    position
  end

  @doc """
  Returns whether the publisher has ended the stream and the image has consumed
  every message up to that point.
  """
  @spec end_of_stream?(t()) :: boolean()
  def end_of_stream?(%__MODULE__{metadata_address: metadata} = image) do
    {:ok, end_of_stream_position} =
      NIF.atomic_get_int64(metadata + @log_end_of_stream_position_offset)

    position(image) >= end_of_stream_position
  end

  defp deliver_controlled(_image, [], _handler, committed, consumed), do: {consumed, committed}

  defp deliver_controlled(
         image,
         [{header, payload} | rest],
         handler,
         committed,
         consumed
       ) do
    end_position = Header.position(header)
    action = apply_action(image, handler.(payload, header), end_position)
    continue_delivery(action, image, rest, handler, committed, consumed, end_position)
  end

  defp apply_action(_image, :abort, _end_position), do: :abort
  defp apply_action(_image, :break, _end_position), do: :break

  defp apply_action(image, :commit, end_position) do
    :ok = NIF.write_int64_ordered(image.subscriber_position_address, end_position)
    :continue
  end

  defp apply_action(_image, _continue, _end_position), do: :continue

  defp continue_delivery(:abort, _image, _rest, _handler, committed, consumed, _end_position),
    do: {consumed, committed}

  defp continue_delivery(:break, _image, _rest, _handler, _committed, consumed, end_position),
    do: {consumed + 1, end_position}

  defp continue_delivery(:continue, image, rest, handler, _committed, consumed, end_position),
    do: deliver_controlled(image, rest, handler, end_position, consumed + 1)
end
