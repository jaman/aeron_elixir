defmodule AeronElixir.Protocol.Frame do
  @moduledoc """
  Aeron frame header encoding and decoding.

  Frame layout (all fields little-endian):
  - frame_length: i32 (includes header)
  - version: i8
  - flags: u8
  - type: i16
  - term_offset: i32
  - session_id: i32
  - stream_id: i32
  - term_id: i32
  - reserved_value: i64
  """

  @frame_header_length 32

  @type frame_type ::
          :pad
          | :data
          | :setup
          | :nak
          | :status_message
          | :rrt
          | :heartbeat
          | :ext

  @type t :: %__MODULE__{
          frame_length: non_neg_integer(),
          version: non_neg_integer(),
          flags: non_neg_integer(),
          type: frame_type(),
          term_offset: non_neg_integer(),
          session_id: integer(),
          stream_id: integer(),
          term_id: integer(),
          reserved_value: integer(),
          initial_term_id: integer() | nil,
          position: integer() | nil
        }

  defstruct [
    :frame_length,
    :version,
    :flags,
    :type,
    :term_offset,
    :session_id,
    :stream_id,
    :term_id,
    :reserved_value,
    :initial_term_id,
    :position
  ]

  @frame_type_codes %{
    pad: 0x00,
    data: 0x01,
    setup: 0x02,
    nak: 0x03,
    status_message: 0x04,
    rrt: 0x05,
    heartbeat: 0x06,
    ext: 0xFF
  }

  @frame_type_names Enum.into(@frame_type_codes, %{}, fn {k, v} -> {v, k} end)

  @flag_begin_fragment 0x80
  @flag_end_fragment 0x40

  @doc """
  Length in bytes of a data frame header.
  """
  @spec frame_header_length() :: pos_integer()
  def frame_header_length, do: @frame_header_length

  @doc """
  Flag bit marking the first fragment of a message.
  """
  @spec flag_begin_fragment() :: pos_integer()
  def flag_begin_fragment, do: @flag_begin_fragment

  @doc """
  Flag bit marking the last fragment of a message.
  """
  @spec flag_end_fragment() :: pos_integer()
  def flag_end_fragment, do: @flag_end_fragment

  @doc """
  Encodes a header into its 32-byte wire form (DataHeaderFlyweight layout).
  """
  @spec encode(t()) :: binary()
  def encode(%__MODULE__{} = frame) do
    type_code = Map.fetch!(@frame_type_codes, frame.type)

    <<frame.frame_length::little-32, frame.version::little-8, frame.flags::little-8,
      type_code::little-16, frame.term_offset::little-32, frame.session_id::little-32,
      frame.stream_id::little-32, frame.term_id::little-32, frame.reserved_value::little-64>>
  end

  @doc """
  Decodes a 32-byte wire header; `initial_term_id` and `position` are left unset.
  """
  @spec decode(binary()) :: {:ok, t()} | {:error, :invalid_frame}
  def decode(
        <<frame_length::little-32, version::little-8, flags::little-8, type_code::little-16,
          term_offset::little-signed-32, session_id::little-signed-32,
          stream_id::little-signed-32, term_id::little-signed-32,
          reserved_value::little-signed-64>>
      ) do
    build_frame(Map.get(@frame_type_names, type_code), %__MODULE__{
      frame_length: frame_length,
      version: version,
      flags: flags,
      term_offset: term_offset,
      session_id: session_id,
      stream_id: stream_id,
      term_id: term_id,
      reserved_value: reserved_value
    })
  end

  def decode(_), do: {:error, :invalid_frame}

  defp build_frame(nil, _frame), do: {:error, :invalid_frame}
  defp build_frame(type, frame), do: {:ok, %{frame | type: type}}

  @doc """
  Returns whether the header carries the begin-fragment flag.
  """
  @spec begin_fragment?(t()) :: boolean()
  def begin_fragment?(%__MODULE__{flags: flags}),
    do: Bitwise.band(flags, @flag_begin_fragment) != 0

  @doc """
  Returns whether the header carries the end-fragment flag.
  """
  @spec end_fragment?(t()) :: boolean()
  def end_fragment?(%__MODULE__{flags: flags}), do: Bitwise.band(flags, @flag_end_fragment) != 0

  @doc """
  Sets the begin-fragment flag.
  """
  @spec set_begin_fragment(t()) :: t()
  def set_begin_fragment(%__MODULE__{flags: flags} = frame) when is_integer(flags) do
    %{frame | flags: Bitwise.bor(flags, @flag_begin_fragment)}
  end

  def set_begin_fragment(%__MODULE__{} = frame), do: %{frame | flags: @flag_begin_fragment}

  @doc """
  Sets the end-fragment flag.
  """
  @spec set_end_fragment(t()) :: t()
  def set_end_fragment(%__MODULE__{flags: flags} = frame) when is_integer(flags) do
    %{frame | flags: Bitwise.bor(flags, @flag_end_fragment)}
  end

  def set_end_fragment(%__MODULE__{} = frame), do: %{frame | flags: @flag_end_fragment}

  @doc """
  Builds a complete unfragmented data frame (header followed by `payload`).
  """
  @spec data_frame(non_neg_integer(), integer(), integer(), integer(), binary()) :: binary()
  def data_frame(term_offset, session_id, stream_id, term_id, payload) do
    payload_length = byte_size(payload)
    frame_length = @frame_header_length + payload_length

    header = %__MODULE__{
      frame_length: frame_length,
      version: 0,
      flags: 0,
      type: :data,
      term_offset: term_offset,
      session_id: session_id,
      stream_id: stream_id,
      term_id: term_id,
      reserved_value: 0
    }

    encode(header) <> payload
  end
end
