defmodule AeronElixir.RingBuffer.MPSC do
  @moduledoc """
  Many-producer single-consumer ring buffer producer over mapped shared memory.

  This is the `to_driver` write path: the client claims space at the tail via an
  atomic compare-and-set, writes the record body, then publishes the record length
  with a release store so the single consumer (the media driver) observes a complete
  record. The buffer lives in the CnC file's mapped region, so all reads and writes
  go through the NIF's atomic accessors against the buffer's absolute base
  address rather than an Elixir-side copy.

  Layout matches Agrona `ManyToOneRingBuffer` / `RingBufferDescriptor`:

      +------------------------- capacity (power of two) -------------------------+
      | record | record | ... (records aligned to 8 bytes)                        |
      +---------------------------------------------------------------------------+
      | tail_position@+128 | head_cache@+256 | head_position@+384 |               |
      | correlation_counter@+512 | consumer_heartbeat@+640 | trailer = 768 bytes  |
      +---------------------------------------------------------------------------+

  Each record is `length:i32@+0, type:i32@+4, body`. The length field carries the
  full record length (header + body); a negative length marks an in-progress claim,
  and `type == -1` marks padding inserted to skip the wrap at the end of the buffer.
  """

  alias AeronElixir.NIF

  @trailer_length 768
  @tail_position_offset 128
  @head_cache_position_offset 256
  @head_position_offset 384
  @correlation_counter_offset 512
  @consumer_heartbeat_offset 640

  @record_header_length 8
  @record_alignment 8
  @padding_msg_type_id -1

  @type t :: %__MODULE__{
          base_address: integer(),
          capacity: pos_integer(),
          max_message_length: pos_integer(),
          tail_address: integer(),
          head_cache_address: integer(),
          head_address: integer(),
          correlation_address: integer(),
          consumer_heartbeat_address: integer()
        }

  defstruct [
    :base_address,
    :capacity,
    :max_message_length,
    :tail_address,
    :head_cache_address,
    :head_address,
    :correlation_address,
    :consumer_heartbeat_address
  ]

  @spec new(integer(), pos_integer()) :: {:ok, t()} | {:error, term()}
  def new(base_address, total_length)
      when is_integer(base_address) and is_integer(total_length) and
             total_length > @trailer_length do
    capacity = total_length - @trailer_length
    build(power_of_two?(capacity), base_address, capacity)
  end

  def new(_base_address, total_length), do: {:error, {:invalid_total_length, total_length}}

  defp build(false, _base_address, capacity), do: {:error, {:capacity_not_power_of_two, capacity}}

  defp build(true, base_address, capacity) do
    trailer = base_address + capacity

    {:ok,
     %__MODULE__{
       base_address: base_address,
       capacity: capacity,
       max_message_length: div(capacity, 8),
       tail_address: trailer + @tail_position_offset,
       head_cache_address: trailer + @head_cache_position_offset,
       head_address: trailer + @head_position_offset,
       correlation_address: trailer + @correlation_counter_offset,
       consumer_heartbeat_address: trailer + @consumer_heartbeat_offset
     }}
  end

  @spec next_correlation_id(t()) :: integer()
  def next_correlation_id(%__MODULE__{correlation_address: address}) do
    {:ok, previous} = NIF.atomic_fetch_add_int64(address, 1)
    previous
  end

  @spec consumer_heartbeat_time(t()) :: integer()
  def consumer_heartbeat_time(%__MODULE__{consumer_heartbeat_address: address}) do
    {:ok, value} = NIF.atomic_get_int64(address)
    value
  end

  @spec write(t(), integer(), binary()) :: :ok | {:error, term()}
  def write(%__MODULE__{max_message_length: max_length}, _message_type, message)
      when byte_size(message) > max_length,
      do: {:error, :message_too_long}

  def write(%__MODULE__{} = ring, message_type, message)
      when is_integer(message_type) and is_binary(message) do
    record_length = @record_header_length + byte_size(message)

    ring
    |> claim_capacity(align(record_length, @record_alignment))
    |> write_claimed(ring, record_length, message_type, message)
  end

  defp write_claimed({:ok, record_index}, ring, record_length, message_type, message) do
    publish_record(ring, record_index, record_length, message_type, message)
    :ok
  end

  defp write_claimed(:insufficient_capacity, _ring, _record_length, _message_type, _message),
    do: {:error, :buffer_full}

  defp publish_record(ring, record_index, record_length, message_type, message) do
    record_address = ring.base_address + record_index
    :ok = NIF.write_int32(length_offset(record_address), -record_length)
    :ok = NIF.write_binary(encoded_msg_offset(record_address), 0, message)
    :ok = NIF.write_int32(type_offset(record_address), message_type)
    :ok = NIF.write_int32(length_offset(record_address), record_length)
  end

  defp claim_capacity(ring, required) do
    mask = ring.capacity - 1
    {:ok, cached_head} = NIF.atomic_get_int64(ring.head_cache_address)
    {:ok, tail} = NIF.atomic_get_int64(ring.tail_address)
    record_index = band(tail, mask)

    with {:ok, head} <- head_with_capacity(ring, required, tail, cached_head),
         {:ok, padding} <- padding_for(ring, mask, required, head, ring.capacity - record_index) do
      commit_claim(ring, required, padding, tail, record_index)
    end
  end

  defp head_with_capacity(ring, required, tail, cached_head)
       when required > ring.capacity - (tail - cached_head) do
    {:ok, head} = NIF.atomic_get_int64(ring.head_address)
    cache_head_if_fits(ring, required, tail, head)
  end

  defp head_with_capacity(_ring, _required, _tail, cached_head), do: {:ok, cached_head}

  defp cache_head_if_fits(ring, required, tail, head)
       when required > ring.capacity - (tail - head),
       do: :insufficient_capacity

  defp cache_head_if_fits(ring, _required, _tail, head) do
    :ok = NIF.write_int64_ordered(ring.head_cache_address, head)
    {:ok, head}
  end

  defp padding_for(ring, mask, required, head, to_buffer_end) when required > to_buffer_end do
    padding_from_head(ring, mask, required, band(head, mask), to_buffer_end)
  end

  defp padding_for(_ring, _mask, _required, _head, _to_buffer_end), do: {:ok, 0}

  defp padding_from_head(ring, mask, required, head_index, to_buffer_end)
       when required > head_index do
    {:ok, refreshed} = NIF.atomic_get_int64(ring.head_address)
    padding_from_refreshed_head(ring, required, band(refreshed, mask), refreshed, to_buffer_end)
  end

  defp padding_from_head(_ring, _mask, _required, _head_index, to_buffer_end),
    do: {:ok, to_buffer_end}

  defp padding_from_refreshed_head(_ring, required, head_index, _refreshed, _to_buffer_end)
       when required > head_index,
       do: :insufficient_capacity

  defp padding_from_refreshed_head(ring, _required, _head_index, refreshed, to_buffer_end) do
    :ok = NIF.write_int64_ordered(ring.head_cache_address, refreshed)
    {:ok, to_buffer_end}
  end

  defp commit_claim(ring, required, padding, tail, record_index) do
    ring.tail_address
    |> NIF.atomic_cas_int64(tail, tail + required + padding)
    |> claim_committed(ring, required, padding, record_index)
  end

  defp claim_committed({:ok, _tail}, ring, _required, padding, record_index),
    do: finalize_claim(ring, padding, record_index)

  defp claim_committed({:failed, _current}, ring, required, _padding, _record_index),
    do: claim_capacity(ring, required)

  defp finalize_claim(_ring, 0, record_index), do: {:ok, record_index}

  defp finalize_claim(ring, padding, record_index) do
    pad_address = ring.base_address + record_index
    :ok = NIF.write_int32(length_offset(pad_address), -padding)
    :ok = NIF.write_int32(type_offset(pad_address), @padding_msg_type_id)
    :ok = NIF.write_int32(length_offset(pad_address), padding)
    {:ok, 0}
  end

  defp length_offset(record_address), do: record_address
  defp type_offset(record_address), do: record_address + 4
  defp encoded_msg_offset(record_address), do: record_address + @record_header_length

  defp align(value, alignment), do: band(value + alignment - 1, -alignment)

  defp band(value, mask), do: Bitwise.band(value, mask)

  defp power_of_two?(value) when value > 0, do: Bitwise.band(value, value - 1) == 0
  defp power_of_two?(_), do: false
end
