defmodule AeronElixir.LogBuffer.TermAppender do
  @moduledoc """
  Pure planning for appending a message to a term buffer.

  Given the active term geometry (`term_offset`, `term_id`, `session_id`,
  `stream_id`, `max_payload_length`, `term_length`) and a payload, `plan/8`
  produces the framing to be written into the active term partition without
  touching memory. The conductor executes the plan against the mapped log via the
  NIF atomics.

  The framing mirrors the base `ExclusivePublication` append path exactly:

  * A message no larger than `max_payload_length` becomes one unfragmented data
    frame with `BEGIN` and `END` flags set (`appendUnfragmentedMessage`).
  * A larger message is split into `max_payload_length`-sized fragments; the first
    carries `BEGIN`, the last carries `END`, the rest carry neither
    (`appendFragmentedMessage`). The resulting offset advances by
    `computeFragmentedFrameLength` (`LogBufferDescriptor`).
  * When the resulting offset would exceed the term length the append cannot
    proceed; the plan reports the padding frame that fills the remainder of the
    term (`handleEndOfLog`) and signals `:rotation_required`.

  Frame headers are produced by `Protocol.Frame`, whose byte layout matches the
  base `DataHeaderFlyweight`. Every frame is aligned to `FRAME_ALIGNMENT` (32).
  """

  alias AeronElixir.Protocol.Frame

  @header_length 32
  @frame_alignment 32

  @type frame_write :: %{
          frame_offset: non_neg_integer(),
          header: binary(),
          payload: binary()
        }

  @type padding_write :: %{
          frame_offset: non_neg_integer(),
          padding_length: non_neg_integer(),
          header: binary()
        }

  @type plan ::
          %{resulting_offset: pos_integer(), frames: [frame_write()]}
          | %{resulting_offset: :rotation_required, padding: padding_write()}

  @spec header_length() :: 32
  def header_length, do: @header_length

  @spec frame_alignment() :: 32
  def frame_alignment, do: @frame_alignment

  @spec plan(
          non_neg_integer(),
          integer(),
          integer(),
          integer(),
          pos_integer(),
          pos_integer(),
          binary(),
          integer()
        ) :: {:ok, plan()}
  def plan(
        term_offset,
        term_id,
        session_id,
        stream_id,
        max_payload_length,
        term_length,
        payload,
        reserved_value
      )
      when term_offset >= 0 and max_payload_length > 0 and term_length > 0 and is_binary(payload) do
    geometry = %{
      term_id: term_id,
      session_id: session_id,
      stream_id: stream_id,
      reserved_value: reserved_value,
      max_payload_length: max_payload_length,
      term_length: term_length
    }

    {:ok, plan_frames(geometry, term_offset, payload)}
  end

  defp plan_frames(%{max_payload_length: max_payload_length} = geometry, term_offset, payload)
       when byte_size(payload) <= max_payload_length do
    frame_length = byte_size(payload) + @header_length
    resulting_offset = term_offset + align(frame_length, @frame_alignment)
    flags = Bitwise.bor(Frame.flag_begin_fragment(), Frame.flag_end_fragment())

    place_frames(geometry, term_offset, resulting_offset, fn ->
      [
        %{
          frame_offset: term_offset,
          header: encode_header(geometry, term_offset, frame_length, flags),
          payload: payload
        }
      ]
    end)
  end

  defp plan_frames(geometry, term_offset, payload) do
    framed_length = fragmented_frame_length(byte_size(payload), geometry.max_payload_length)
    resulting_offset = term_offset + framed_length

    place_frames(geometry, term_offset, resulting_offset, fn ->
      build_fragments(geometry, term_offset, payload, Frame.flag_begin_fragment(), [])
    end)
  end

  defp place_frames(
         %{term_length: term_length} = geometry,
         term_offset,
         resulting_offset,
         _frames
       )
       when resulting_offset > term_length do
    end_of_log_plan(geometry, term_offset)
  end

  defp place_frames(_geometry, _term_offset, resulting_offset, frames) do
    %{resulting_offset: resulting_offset, frames: frames.()}
  end

  defp build_fragments(
         %{max_payload_length: max_payload_length} = geometry,
         frame_offset,
         payload,
         flags,
         acc
       )
       when byte_size(payload) <= max_payload_length do
    frame_flags = Bitwise.bor(flags, Frame.flag_end_fragment())
    Enum.reverse([fragment(geometry, frame_offset, payload, frame_flags) | acc])
  end

  defp build_fragments(
         %{max_payload_length: max_payload_length} = geometry,
         frame_offset,
         payload,
         flags,
         acc
       ) do
    <<chunk::binary-size(^max_payload_length), rest::binary>> = payload
    next_offset = frame_offset + align(max_payload_length + @header_length, @frame_alignment)

    build_fragments(geometry, next_offset, rest, 0, [
      fragment(geometry, frame_offset, chunk, flags) | acc
    ])
  end

  defp fragment(geometry, frame_offset, chunk, flags) do
    header = encode_header(geometry, frame_offset, byte_size(chunk) + @header_length, flags)
    %{frame_offset: frame_offset, header: header, payload: chunk}
  end

  defp end_of_log_plan(geometry, term_offset) do
    padding_length = geometry.term_length - term_offset

    %{
      resulting_offset: :rotation_required,
      padding: %{
        frame_offset: term_offset,
        padding_length: padding_length,
        header: encode_padding_header(geometry, term_offset, padding_length)
      }
    }
  end

  defp encode_header(geometry, term_offset, frame_length, flags) do
    %Frame{
      frame_length: frame_length,
      version: 0,
      flags: flags,
      type: :data,
      term_offset: term_offset,
      session_id: geometry.session_id,
      stream_id: geometry.stream_id,
      term_id: geometry.term_id,
      reserved_value: geometry.reserved_value
    }
    |> Frame.encode()
  end

  defp encode_padding_header(geometry, term_offset, padding_length) do
    %Frame{
      frame_length: padding_length,
      version: 0,
      flags: 0,
      type: :pad,
      term_offset: term_offset,
      session_id: geometry.session_id,
      stream_id: geometry.stream_id,
      term_id: geometry.term_id,
      reserved_value: 0
    }
    |> Frame.encode()
  end

  defp fragmented_frame_length(length, max_payload_length) do
    full_frames = div(length, max_payload_length)

    full_frames * (max_payload_length + @header_length) +
      last_frame_length(rem(length, max_payload_length))
  end

  defp last_frame_length(0), do: 0

  defp last_frame_length(remaining_payload),
    do: align(remaining_payload + @header_length, @frame_alignment)

  defp align(value, alignment), do: Bitwise.band(value + alignment - 1, -alignment)
end
