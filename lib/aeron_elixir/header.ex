defmodule AeronElixir.Header do
  @moduledoc """
  The header a poll handler receives with each message, left encoded so that a
  handler pays only for the fields it reads.

  A header is a 44-byte binary, all fields little-endian:

    * bytes 0–31: the data frame header of the message's final fragment —
      `frame_length` (i32), `version` (u8), `flags` (u8), `type` (u16),
      `term_offset` (i32), `session_id` (i32), `stream_id` (i32), `term_id` (i32),
      `reserved_value` (i64)
    * bytes 32–39: `position` (i64), the stream position just past the message
    * bytes 40–43: `initial_term_id` (i32) of the image

  Each function reads one field with a single binary match. A handler may also
  match the binary itself:

      fn payload, <<_::binary-12, session_id::little-signed-32, _::binary>> ->
        {session_id, payload}
      end

  `frame/1` decodes every field into an `AeronElixir.Protocol.Frame`.
  """

  alias AeronElixir.Protocol.Frame

  @type t :: <<_::352>>

  @doc "Returns the length of the final fragment's frame, header included."
  @spec frame_length(t()) :: integer()
  def frame_length(<<frame_length::little-signed-32, _::binary-40>>), do: frame_length

  @doc "Returns the final fragment's flags byte: `0x80` begin, `0x40` end."
  @spec flags(t()) :: non_neg_integer()
  def flags(<<_::binary-5, flags::8, _::binary-38>>), do: flags

  @doc "Returns the frame type code; `1` for a data frame."
  @spec type(t()) :: non_neg_integer()
  def type(<<_::binary-6, type::little-16, _::binary-36>>), do: type

  @doc "Returns the final fragment's offset within its term."
  @spec term_offset(t()) :: integer()
  def term_offset(<<_::binary-8, term_offset::little-signed-32, _::binary-32>>), do: term_offset

  @doc "Returns the publisher's session id."
  @spec session_id(t()) :: integer()
  def session_id(<<_::binary-12, session_id::little-signed-32, _::binary-28>>), do: session_id

  @doc "Returns the stream id."
  @spec stream_id(t()) :: integer()
  def stream_id(<<_::binary-16, stream_id::little-signed-32, _::binary-24>>), do: stream_id

  @doc "Returns the term id of the final fragment."
  @spec term_id(t()) :: integer()
  def term_id(<<_::binary-20, term_id::little-signed-32, _::binary-20>>), do: term_id

  @doc "Returns the reserved value the publisher wrote with the message."
  @spec reserved_value(t()) :: integer()
  def reserved_value(<<_::binary-24, reserved_value::little-signed-64, _::binary-12>>),
    do: reserved_value

  @doc "Returns the stream position just past the message."
  @spec position(t()) :: integer()
  def position(<<_::binary-32, position::little-signed-64, _::binary-4>>), do: position

  @doc "Returns the image's initial term id."
  @spec initial_term_id(t()) :: integer()
  def initial_term_id(<<_::binary-40, initial_term_id::little-signed-32>>), do: initial_term_id

  @doc """
  Decodes every field into an `AeronElixir.Protocol.Frame`, with `position` and
  `initial_term_id` set.
  """
  @spec frame(t()) :: Frame.t()
  def frame(
        <<frame_bytes::binary-32, position::little-signed-64, initial_term_id::little-signed-32>>
      ) do
    {:ok, frame} = Frame.decode(frame_bytes)
    %{frame | position: position, initial_term_id: initial_term_id}
  end
end
