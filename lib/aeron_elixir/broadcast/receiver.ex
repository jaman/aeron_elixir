defmodule AeronElixir.Broadcast.Receiver do
  @moduledoc """
  Receiver for an Aeron broadcast buffer (driver to all clients).

  The `to_clients` buffer is a broadcast buffer rather than a ring buffer: a single
  transmitter writes records that every receiver reads independently, tracking its own
  cursor. Records are read straight out of the mapped buffer, so the counters a read
  is validated against are the transmitter's current values rather than a copy taken
  earlier.

  Each record is read, its payload copied, and the transmitter's tail-intent counter
  re-read afterwards. A record the transmitter overwrote during that read, or whose
  length is shorter than its header or extends past the buffer capacity, yields
  `{:error, :lapped}`. A caller that receives it has lost every record between its
  cursor and the transmitter's latest, and resumes from `latest_position/1`.

  Buffer layout matches the Agrona / C `aeron_broadcast_descriptor`:

      +------------------------------- capacity (power of two) ---------------------+
      | record | record | ... (records aligned to 8 bytes)                          |
      +-----------------------------------------------------------------------------+
      | tail_intent_counter : i64 | tail_counter : i64 | latest_counter : i64 | pad |
      +------------------------------ trailer = 128 bytes --------------------------+

  Each record is `length : i32, msg_type_id : i32, payload`. `length` includes the
  8-byte header; records are aligned up to 8 bytes. A `msg_type_id` of -1 marks
  padding inserted to skip the wrap at the end of the buffer, and the reader
  continues at the start of the buffer.
  """

  alias AeronElixir.NIF

  @trailer_length 128
  @latest_counter_offset 16
  @default_limit 16

  @type broadcast_record :: {non_neg_integer(), binary()}

  @type t :: %__MODULE__{address: integer(), capacity: pos_integer()}

  defstruct [:address, :capacity]

  @doc """
  Builds a receiver over the broadcast buffer mapped at `address`.

  `length` is the whole mapped region including the 128-byte trailer; the record
  capacity is what remains and must be a power of two.
  """
  @spec new(integer(), pos_integer()) :: t()
  def new(address, length) when is_integer(address) and is_integer(length) do
    %__MODULE__{address: address, capacity: length - @trailer_length}
  end

  @doc """
  Reads the transmitter's latest record position.

  A receiver that has been lapped resumes from this position, and a new receiver
  starts from it, matching the Agrona and C clients.
  """
  @spec latest_position(t()) :: integer()
  def latest_position(%__MODULE__{address: address, capacity: capacity}) do
    {:ok, position} = NIF.atomic_get_int64(address + capacity + @latest_counter_offset)
    position
  end

  @doc """
  Reads up to `limit` records from `position`.

  Returns `{:ok, records, next_position}` with the records in transmitter order, or
  `{:error, :lapped}` when the transmitter has overrun the cursor.
  """
  @spec receive(t(), integer(), pos_integer()) ::
          {:ok, [broadcast_record()], integer()} | {:error, :lapped}
  def receive(receiver, position, limit \\ @default_limit)

  def receive(%__MODULE__{address: address, capacity: capacity}, position, limit)
      when is_integer(position) and is_integer(limit) and limit > 0 do
    NIF.broadcast_receive(address, capacity, position, limit)
  end
end
