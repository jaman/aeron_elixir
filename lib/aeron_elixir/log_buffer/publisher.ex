defmodule AeronElixir.LogBuffer.Publisher do
  @moduledoc """
  Runtime publication over a mapped log buffer.

  Holds the geometry the driver assigned to a publication (the log base address,
  term length, session/stream id, payload limit, position geometry and the
  publication-limit counter address) and appends data frames into the active term
  partition via the NIF.

  `publish/3` and `publish_n/4` follow the base concurrent `Publication.offer`
  contract: the position is checked against the publication-limit counter, term
  space is reserved with an atomic add on the active partition's tail, the frame
  is written with its length released last so a reader only observes a complete
  frame, and a message that does not fit the remaining term writes padding and
  rotates to the next partition with a CAS. Many processes may publish through the
  same struct concurrently; each message occupies its own reserved span.

  Payloads longer than `max_payload_length` are fragmented across several frames
  by `TermAppender` and appended one frame at a time.

  All metadata offsets match the base `LogBufferDescriptor`.

  `region` keeps the log mapping alive, and the log mapping keeps the CnC mapping
  that holds the publication-limit counter alive, so every address in a handle
  stays valid for as long as the handle is reachable.
  """

  alias AeronElixir.LogBuffer.Mapping
  alias AeronElixir.LogBuffer.TermAppender
  alias AeronElixir.NIF

  @partition_count 3
  @log_meta_data_length 4096
  @term_tail_counters_offset 0
  @active_term_count_offset 24
  @size_of_long 8
  @header_length 32
  @frame_length_field_offset 0
  @log_is_connected_offset 136
  @offer_closed -4
  @offer_payload_too_long -6

  @type t :: %__MODULE__{
          registration_id: integer(),
          log_base_address: integer(),
          metadata_address: integer(),
          term_length: pos_integer(),
          session_id: integer(),
          stream_id: integer(),
          initial_term_id: integer(),
          max_payload_length: pos_integer(),
          position_bits_to_shift: pos_integer(),
          max_possible_position: pos_integer(),
          limit_counter_address: integer(),
          closed_address: integer(),
          geometry: binary(),
          region: reference()
        }

  defstruct [
    :registration_id,
    :log_base_address,
    :metadata_address,
    :term_length,
    :session_id,
    :stream_id,
    :initial_term_id,
    :max_payload_length,
    :position_bits_to_shift,
    :max_possible_position,
    :limit_counter_address,
    :closed_address,
    :geometry,
    :region
  ]

  @doc """
  Builds the handle for a mapped publication log from the driver's response fields and the log descriptor.
  """
  @spec new(integer(), Mapping.t(), integer(), integer(), struct(), integer()) :: t()
  def new(
        registration_id,
        %Mapping{} = mapping,
        session_id,
        stream_id,
        descriptor,
        limit_counter_address
      ) do
    publication = %__MODULE__{
      region: mapping.region,
      registration_id: registration_id,
      log_base_address: mapping.base_address,
      metadata_address: mapping.base_address + (mapping.length - @log_meta_data_length),
      term_length: descriptor.term_buffer_length,
      session_id: session_id,
      stream_id: stream_id,
      initial_term_id: descriptor.initial_term_id,
      max_payload_length: descriptor.max_payload_length,
      position_bits_to_shift: descriptor.position_bits_to_shift,
      max_possible_position: descriptor.max_possible_position,
      limit_counter_address: limit_counter_address,
      closed_address: NIF.closed_flag_address(mapping.region)
    }

    %{publication | geometry: encode_geometry(publication)}
  end

  defp encode_geometry(publication) do
    <<
      publication.metadata_address::native-signed-64,
      publication.log_base_address::native-signed-64,
      publication.term_length::native-signed-64,
      publication.session_id::native-signed-64,
      publication.stream_id::native-signed-64,
      publication.initial_term_id::native-signed-64,
      publication.position_bits_to_shift::native-signed-64,
      publication.max_possible_position::native-signed-64,
      publication.limit_counter_address::native-signed-64,
      publication.closed_address::native-signed-64,
      publication.max_payload_length::native-signed-64
    >>
  end

  @doc """
  Publishes `payload` (a binary or iodata) as one message.

  Payloads up to `max_payload_length` bytes are written into the term buffer in a
  single native call, iodata segments copied directly into the frame. Larger
  payloads are fragmented across several frames.
  """
  @spec publish(t(), iodata(), integer()) :: {:ok, integer()} | {:error, term()}
  def publish(%__MODULE__{geometry: geometry} = publication, payload, reserved_value)
      when (is_binary(payload) or is_list(payload)) and is_integer(reserved_value) do
    geometry
    |> NIF.offer(payload, reserved_value)
    |> offer_result(publication, payload, reserved_value)
  end

  defp offer_fragmented(true, _publication, _payload, _reserved_value), do: {:error, :closed}

  defp offer_fragmented(false, publication, payload, reserved_value),
    do: offer_elixir(publication, IO.iodata_to_binary(payload), reserved_value)

  @doc """
  Publishes `count` copies of `payload` in a single NIF call.

  Returns the number of messages appended before the publication back-pressured
  (`count` on full success). Unfragmented payloads are batched through
  `NIF.offer_n/4`; larger payloads fall back to repeated `publish/3`.
  """
  @spec publish_n(t(), binary(), integer(), non_neg_integer()) ::
          non_neg_integer() | {:error, :closed}
  def publish_n(%__MODULE__{} = publication, payload, reserved_value, count)
      when is_binary(payload) and is_integer(reserved_value) and is_integer(count) and
             count >= 0 and byte_size(payload) <= publication.max_payload_length do
    publication.geometry
    |> NIF.offer_n(payload, reserved_value, count)
    |> batch_result()
  end

  def publish_n(%__MODULE__{} = publication, payload, reserved_value, count)
      when is_binary(payload) and is_integer(reserved_value) and is_integer(count) and count >= 0 do
    Enum.reduce_while(1..count//1, 0, fn _, appended ->
      count_appended(publish(publication, payload, reserved_value), appended)
    end)
  end

  defp count_appended({:ok, _position}, appended), do: {:cont, appended + 1}
  defp count_appended({:error, :closed}, 0), do: {:halt, {:error, :closed}}
  defp count_appended({:error, _reason}, appended), do: {:halt, appended}

  defp batch_result(@offer_closed), do: {:error, :closed}
  defp batch_result(appended), do: max(appended, 0)

  @doc """
  Publishes each element of `payloads` (binaries or iodata) as its own message,
  in order, in a single NIF call.

  Returns the number of messages appended before the publication back-pressured
  (`length(payloads)` on full success); the caller resumes from the first payload
  not appended. Every payload must be at most `max_payload_length` bytes.
  """
  @spec publish_list(t(), [iodata()], integer()) :: non_neg_integer() | {:error, :closed}
  def publish_list(%__MODULE__{geometry: geometry}, payloads, reserved_value)
      when is_list(payloads) and is_integer(reserved_value) do
    geometry
    |> NIF.offer_list(payloads, reserved_value)
    |> batch_result()
  end

  @doc """
  Returns whether the driver reports at least one connected subscriber for the
  publication (the log's `isConnected` flag).
  """
  @spec connected?(t()) :: boolean()
  def connected?(%__MODULE__{metadata_address: metadata}) do
    {:ok, flag} = NIF.read_int32(metadata + @log_is_connected_offset)
    flag == 1
  end

  @doc """
  Returns whether the publication has been closed, either with `close/1` or
  because the client lost its media driver. Every publish on a closed
  publication returns `{:error, :closed}`.
  """
  @spec closed?(t()) :: boolean()
  def closed?(%__MODULE__{closed_address: address}) do
    {:ok, flag} = NIF.read_int32(address)
    flag != 0
  end

  @doc """
  Returns the publication's current stream position, taken from the active
  partition's tail.
  """
  @spec position(t()) :: integer()
  def position(%__MODULE__{} = publication) do
    active_index = active_partition_index(publication)
    raw_tail = raw_tail(publication, active_index)
    term_id = term_id_from(raw_tail)
    term_begin_position(publication, term_id) + term_offset_from(raw_tail)
  end

  @doc """
  Returns the position up to which the publication may currently append before
  back-pressure applies (the publication-limit counter).
  """
  @spec position_limit(t()) :: integer()
  def position_limit(%__MODULE__{limit_counter_address: address}) do
    {:ok, limit} = NIF.atomic_get_int64(address)
    limit
  end

  defp offer_result(position, _publication, _payload, _reserved_value) when position >= 0,
    do: {:ok, position}

  defp offer_result(@offer_payload_too_long, publication, payload, reserved_value),
    do: offer_fragmented(closed?(publication), publication, payload, reserved_value)

  defp offer_result(-5, _publication, _payload, _reserved_value),
    do: {:error, :max_position_exceeded}

  defp offer_result(-3, _publication, _payload, _reserved_value), do: {:error, :admin_action}
  defp offer_result(@offer_closed, _publication, _payload, _reserved_value), do: {:error, :closed}

  defp offer_result(_limit_reached, publication, _payload, _reserved_value),
    do: {:error, back_pressure_status(publication)}

  defp back_pressure_status(publication), do: back_pressure_status_when(connected?(publication))

  defp back_pressure_status_when(true), do: :back_pressured
  defp back_pressure_status_when(false), do: :not_connected

  defp offer_elixir(publication, payload, reserved_value) do
    active_index = active_partition_index(publication)
    raw_tail = raw_tail(publication, active_index)
    term_id = term_id_from(raw_tail)
    term_offset = term_offset_from(raw_tail)
    term_begin_position = term_begin_position(publication, term_id)
    position = term_begin_position + term_offset

    {:ok, limit} = NIF.atomic_get_int64(publication.limit_counter_address)

    offer_within_limit(
      position < limit,
      publication,
      %{
        active_index: active_index,
        term_id: term_id,
        term_offset: term_offset,
        term_begin_position: term_begin_position
      },
      payload,
      reserved_value
    )
  end

  defp offer_within_limit(false, publication, _term, _payload, _reserved_value),
    do: {:error, back_pressure_status(publication)}

  defp offer_within_limit(true, publication, term, payload, reserved_value),
    do: append(publication, term, payload, reserved_value)

  defp append(publication, term, payload, reserved_value) do
    {:ok, plan} =
      TermAppender.plan(
        term.term_offset,
        term.term_id,
        publication.session_id,
        publication.stream_id,
        publication.max_payload_length,
        publication.term_length,
        payload,
        reserved_value
      )

    execute_plan(plan, publication, term)
  end

  defp execute_plan(%{resulting_offset: :rotation_required, padding: padding}, publication, term) do
    handle_end_of_log(publication, term, padding)
  end

  defp execute_plan(%{resulting_offset: resulting_offset, frames: frames}, publication, term) do
    store_tail(publication, term.active_index, term.term_id, resulting_offset)
    term_base = term_base_address(publication, term.active_index)
    Enum.each(frames, &write_frame(term_base, &1))
    {:ok, term.term_begin_position + resulting_offset}
  end

  defp handle_end_of_log(
         %{term_length: term_length, max_possible_position: max_possible_position},
         %{term_begin_position: term_begin_position},
         _padding
       )
       when term_begin_position + term_length >= max_possible_position,
       do: {:error, :max_position_exceeded}

  defp handle_end_of_log(publication, term, padding) do
    store_tail(publication, term.active_index, term.term_id, publication.term_length)
    maybe_write_padding(publication, term.active_index, padding)
    rotate(publication, term.active_index, term.term_id)
    {:error, :admin_action}
  end

  defp maybe_write_padding(_publication, _active_index, %{padding_length: 0}), do: :ok

  defp maybe_write_padding(publication, active_index, padding) do
    write_padding(term_base_address(publication, active_index), padding)
  end

  defp write_frame(term_base, frame) do
    frame_address = term_base + frame.frame_offset
    frame_length = byte_size(frame.header) + byte_size(frame.payload)

    :ok = NIF.write_binary(frame_address, 0, negate_length_header(frame.header))
    :ok = NIF.write_binary(frame_address + @header_length, 0, frame.payload)
    :ok = NIF.write_int32_ordered(frame_address + @frame_length_field_offset, frame_length)
  end

  defp write_padding(term_base, padding) do
    frame_address = term_base + padding.frame_offset

    :ok = NIF.write_binary(frame_address, 0, negate_length_header(padding.header))

    :ok =
      NIF.write_int32_ordered(frame_address + @frame_length_field_offset, padding.padding_length)
  end

  defp negate_length_header(<<frame_length::little-signed-32, rest::binary>>) do
    <<-frame_length::little-signed-32>> <> rest
  end

  defp store_tail(publication, active_index, term_id, term_offset) do
    NIF.write_int64_ordered(
      tail_counter_address(publication, active_index),
      pack_tail(term_id, term_offset)
    )
  end

  defp rotate(publication, active_index, term_id) do
    next_index = rem(active_index + 1, @partition_count)
    next_term_id = term_id + 1
    next_term_count = next_term_id - publication.initial_term_id

    NIF.write_int64(
      tail_counter_address(publication, next_index),
      pack_tail(next_term_id, 0)
    )

    NIF.write_int32_ordered(
      publication.metadata_address + @active_term_count_offset,
      next_term_count
    )
  end

  defp active_partition_index(publication) do
    {:ok, term_count} = NIF.read_int32(publication.metadata_address + @active_term_count_offset)
    rem(term_count, @partition_count)
  end

  defp raw_tail(publication, active_index) do
    {:ok, value} = NIF.read_int64(tail_counter_address(publication, active_index))
    value
  end

  defp tail_counter_address(publication, partition_index) do
    publication.metadata_address + @term_tail_counters_offset + partition_index * @size_of_long
  end

  defp term_base_address(publication, partition_index) do
    publication.log_base_address + partition_index * publication.term_length
  end

  defp term_begin_position(publication, term_id) do
    Bitwise.bsl(term_id - publication.initial_term_id, publication.position_bits_to_shift)
  end

  defp term_id_from(raw_tail), do: Bitwise.bsr(raw_tail, 32)

  defp term_offset_from(raw_tail), do: Bitwise.band(raw_tail, 0xFFFFFFFF)

  defp pack_tail(term_id, term_offset) do
    Bitwise.bor(Bitwise.bsl(term_id, 32), Bitwise.band(term_offset, 0xFFFFFFFF))
  end
end
