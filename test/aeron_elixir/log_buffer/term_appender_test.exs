defmodule AeronElixir.LogBuffer.TermAppenderTest do
  use ExUnit.Case

  alias AeronElixir.LogBuffer.TermAppender
  alias AeronElixir.Protocol.Frame

  @header_length 32
  @frame_alignment 32

  @session_id 0x4242
  @stream_id 7
  @term_id 3
  @term_length 65_536
  @max_payload_length 1376

  defp plan(payload, opts \\ []) do
    TermAppender.plan(
      Keyword.get(opts, :term_offset, 0),
      @term_id,
      @session_id,
      @stream_id,
      @max_payload_length,
      Keyword.get(opts, :term_length, @term_length),
      payload,
      Keyword.get(opts, :reserved_value, 0)
    )
  end

  describe "plan/8 unfragmented message" do
    test "produces a single frame whose header matches the base layout" do
      payload = "hello world"

      assert {:ok, %{resulting_offset: resulting_offset, frames: [frame]}} = plan(payload)

      frame_length = @header_length + byte_size(payload)
      assert resulting_offset == align(frame_length, @frame_alignment)

      assert frame.frame_offset == 0
      assert frame.payload == payload

      assert {:ok, header} = Frame.decode(frame.header)
      assert header.frame_length == frame_length
      assert header.type == :data
      assert header.version == 0
      assert header.term_offset == 0
      assert header.session_id == @session_id
      assert header.stream_id == @stream_id
      assert header.term_id == @term_id
      assert header.reserved_value == 0
      assert Frame.begin_fragment?(header)
      assert Frame.end_fragment?(header)
    end

    test "encodes the reserved value into the frame header" do
      assert {:ok, %{frames: [frame]}} = plan("abc", reserved_value: 0x0102030405060708)
      assert {:ok, header} = Frame.decode(frame.header)
      assert header.reserved_value == 0x0102030405060708
    end

    test "places the frame at a non-zero starting term offset" do
      assert {:ok, %{frames: [frame], resulting_offset: resulting_offset}} =
               plan("xyz", term_offset: 64)

      assert frame.frame_offset == 64
      assert {:ok, header} = Frame.decode(frame.header)
      assert header.term_offset == 64
      assert resulting_offset == 64 + align(@header_length + 3, @frame_alignment)
    end
  end

  describe "plan/8 fragmented message" do
    test "splits a payload larger than max_payload_length into begin/middle/end frames" do
      payload = :binary.copy(<<0xAB>>, @max_payload_length * 2 + 10)

      assert {:ok, %{frames: frames, resulting_offset: resulting_offset}} = plan(payload)
      assert length(frames) == 3

      [first, middle, last] = frames

      assert {:ok, first_header} = Frame.decode(first.header)
      assert Frame.begin_fragment?(first_header)
      refute Frame.end_fragment?(first_header)
      assert byte_size(first.payload) == @max_payload_length

      assert {:ok, middle_header} = Frame.decode(middle.header)
      refute Frame.begin_fragment?(middle_header)
      refute Frame.end_fragment?(middle_header)
      assert byte_size(middle.payload) == @max_payload_length

      assert {:ok, last_header} = Frame.decode(last.header)
      refute Frame.begin_fragment?(last_header)
      assert Frame.end_fragment?(last_header)
      assert byte_size(last.payload) == 10

      reassembled = first.payload <> middle.payload <> last.payload
      assert reassembled == payload

      expected_offset =
        align(@max_payload_length + @header_length, @frame_alignment) * 2 +
          align(10 + @header_length, @frame_alignment)

      assert resulting_offset == expected_offset
    end

    test "every fragment carries the same term id and session id" do
      payload = :binary.copy(<<1>>, @max_payload_length + 1)

      assert {:ok, %{frames: frames}} = plan(payload)

      for frame <- frames do
        assert {:ok, header} = Frame.decode(frame.header)
        assert header.term_id == @term_id
        assert header.session_id == @session_id
        assert header.stream_id == @stream_id
      end
    end
  end

  describe "plan/8 end-of-log" do
    test "reports padding when the message does not fit in the remaining term space" do
      term_length = 65_536
      term_offset = term_length - 64

      assert {:ok, %{padding: padding, resulting_offset: :rotation_required}} =
               plan("a sufficiently long payload that overruns the term tail end",
                 term_offset: term_offset,
                 term_length: term_length
               )

      assert padding.frame_offset == term_offset
      assert padding.padding_length == term_length - term_offset

      assert {:ok, header} = Frame.decode(padding.header)
      assert header.type == :pad
      assert header.frame_length == term_length - term_offset
      assert header.term_id == @term_id
    end

    test "reports a zero-length padding at a term offset that exactly fills the term" do
      term_length = 65_536

      assert {:ok, %{padding: padding, resulting_offset: :rotation_required}} =
               plan("x", term_offset: term_length, term_length: term_length)

      assert padding.frame_offset == term_length
      assert padding.padding_length == 0
    end
  end

  defp align(value, alignment), do: Bitwise.band(value + alignment - 1, -alignment)
end
