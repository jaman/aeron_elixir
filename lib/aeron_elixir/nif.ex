defmodule AeronElixir.NIF do
  @moduledoc """
  Memory mapping, atomics and log-buffer reads and writes on the shared memory
  the client and the media driver use.

  The functions are a C NIF built from `c_src/aeron_elixir_nif.c` into
  `priv/lib/aeron_elixir_nif.so` by `mix compile` and loaded when this module
  loads. Mapped files are represented by their base virtual address as an
  integer, and the accessors operate on absolute addresses, so the same functions
  serve the CnC file, the to-driver ring buffer, the to-clients broadcast buffer
  and the log buffers.
  """

  @on_load :load_nif

  @doc false
  def load_nif do
    :aeron_elixir
    |> :code.priv_dir()
    |> Path.join("lib/aeron_elixir_nif")
    |> String.to_charlist()
    |> :erlang.load_nif(0)
  end

  @doc """
  Maps the CnC file at `path`, returning `{:ok, region, base_address}`.

  `region` is a reference owning the mapping: the mapping stays valid while any
  term referencing it is reachable, and is released when the last reference is
  collected. `base_address` is only meaningful for as long as `region` is held.
  """
  def map_cnc(_path), do: :erlang.nif_error(:not_loaded)

  @doc """
  Maps the log file at `path`, returning `{:ok, region, base_address, length}`.

  `region` owns the mapping on the same terms as `map_cnc/1`.
  """
  def map_log(_path), do: :erlang.nif_error(:not_loaded)

  @doc """
  Returns the number of mapped regions currently held.

  A gauge of the log and CnC mappings alive in this VM; it falls as regions are
  released by garbage collection.
  """
  def mapped_region_count, do: :erlang.nif_error(:not_loaded)

  @doc """
  Returns the address of the closed flag carried by `region`, a reference from
  `map_log/1`.

  The flag is 0 while the region's publication or image is open. The client
  conductor stores 1 with `write_int32_ordered/2` when it closes the resource;
  from then on `offer/3`, `offer_n/4` and `offer_list/3` return `-4` and the poll
  functions read nothing. The address is valid while `region` is held.
  """
  def closed_flag_address(_region), do: :erlang.nif_error(:not_loaded)

  @doc """
  Makes the log `region` keep the CnC `parent` region mapped for as long as the
  log region itself is mapped, and returns `:ok`.

  A log handle reads counters, such as the publication limit and the subscriber
  position, from the CnC mapping; with the parent retained, those addresses stay
  valid for as long as any term referencing the log region is reachable. Returns
  `{:error, :parent_already_set}` when `region` already retains a parent.
  """
  def retain_parent_region(_region, _parent), do: :erlang.nif_error(:not_loaded)
  def read_metadata(_resource), do: :erlang.nif_error(:not_loaded)
  def get_buffer_address(_resource, _offset), do: :erlang.nif_error(:not_loaded)
  def read_int32(_address), do: :erlang.nif_error(:not_loaded)
  def read_int64(_address), do: :erlang.nif_error(:not_loaded)
  def write_int32(_address, _value), do: :erlang.nif_error(:not_loaded)
  def write_int64(_address, _value), do: :erlang.nif_error(:not_loaded)
  def write_int32_ordered(_address, _value), do: :erlang.nif_error(:not_loaded)
  def write_int64_ordered(_address, _value), do: :erlang.nif_error(:not_loaded)
  def atomic_get_int64(_address), do: :erlang.nif_error(:not_loaded)
  def atomic_fetch_add_int64(_address, _delta), do: :erlang.nif_error(:not_loaded)
  def atomic_cas_int64(_address, _expected, _desired), do: :erlang.nif_error(:not_loaded)
  def read_binary(_address, _length, _offset), do: :erlang.nif_error(:not_loaded)
  def write_binary(_address, _offset, _data), do: :erlang.nif_error(:not_loaded)

  @doc """
  Appends one data frame carrying `payload` (a binary or iodata, written segment
  by segment into the frame) to the publication described by `geometry` (see
  the `geometry` field of `AeronElixir.LogBuffer.Publisher`).

  Returns the new stream position, `-2` when back-pressured, `-3` when a term
  rotation was still in progress after retrying, `-4` when the publication has
  been closed, `-5` when the log's maximum position is exceeded, or `-6` when the
  payload is longer than the publication's `max_payload_length` and must be
  fragmented; nothing is written in that case. Safe to call
  concurrently from many processes on the same publication.
  """
  def offer(_geometry, _payload, _reserved_value), do: :erlang.nif_error(:not_loaded)

  @doc """
  Appends up to `count` copies of `payload` and returns how many were appended
  before the first non-positive `offer/3` result, or `-4` when the publication
  was already closed.
  """
  def offer_n(_geometry, _payload, _reserved_value, _count), do: :erlang.nif_error(:not_loaded)

  @doc """
  Appends each element of `payloads` (a list of binaries or iodata) as its own message, in order, and
  returns how many were appended before the first non-positive `offer/3` result,
  or `-4` when the publication was already closed. Every payload must fit in one
  frame.
  """
  def offer_list(_geometry, _payloads, _reserved_value), do: :erlang.nif_error(:not_loaded)

  @doc """
  Reads up to `limit` records from the broadcast buffer mapped at `address`,
  starting at `position`.

  Returns `{:ok, [{type_id, payload}], next_position}`, or `{:error, :lapped}` when
  the transmitter has overrun the cursor or left a record unreadable. `capacity` is
  the record region's length, excluding the trailer.
  """
  def broadcast_receive(_address, _capacity, _position, _limit),
    do: :erlang.nif_error(:not_loaded)

  @doc """
  Advances the image described by `geometry` (see
  the `geometry` field of `AeronElixir.LogBuffer.Subscriber`) past up to `fragment_limit`
  frames without copying them and returns the number of data frames skipped.
  """
  def poll_count(_geometry, _fragment_limit), do: :erlang.nif_error(:not_loaded)

  @doc """
  Reads up to `fragment_limit` whole messages from the image described by
  `geometry`, reassembling fragmented messages, and returns `{count, payloads}`.

  A call that returns a single payload of 64 bytes or less returns it as a binary
  of its own. Otherwise all payloads of the call share a single binary
  allocation, and each element of `payloads` is a sub-binary of it. Only one
  process may poll a given image.
  """
  def poll_collect(_geometry, _fragment_limit), do: :erlang.nif_error(:not_loaded)

  @doc """
  Like `poll_collect/2` but returns `{count, [{header, payload}]}`, where
  `header` is the 32-byte data frame header of the message's final fragment,
  decodable with `AeronElixir.Protocol.Frame.decode/1`.
  """
  def poll_collect_framed(_geometry, _fragment_limit), do: :erlang.nif_error(:not_loaded)

  @doc """
  Like `poll_collect_framed/2` but leaves the subscriber position untouched and
  returns `{count, [{header, payload, end_position}]}`, where `end_position` is
  the stream position immediately after that message. The caller advances the
  subscriber position itself with `write_int64_ordered/2`.
  """
  def peek_collect_framed(_geometry, _fragment_limit), do: :erlang.nif_error(:not_loaded)

  @doc """
  Reads the next whole message from the image described by `geometry`,
  reassembling it when fragmented, and returns its payload as a binary of its
  own, or `nil` when no complete message is waiting or the image is closed.

  The subscriber position advances past the message, and past any padding
  before it. Only one process may poll a given image.
  """
  def poll_next(_geometry), do: :erlang.nif_error(:not_loaded)
end
