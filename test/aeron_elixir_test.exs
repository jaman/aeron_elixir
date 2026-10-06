defmodule AeronElixirTest do
  use ExUnit.Case

  alias AeronElixir.Protocol.Frame
  alias AeronElixir.Protocol.URI

  doctest URI
  doctest Frame

  describe "URI parsing" do
    test "parses UDP channel URI" do
      uri = "aeron:udp?endpoint=224.0.1.1:40456|interface=192.168.0.1"
      assert {:ok, parsed} = URI.parse(uri)
      assert parsed.transport == :udp
      assert parsed.endpoint == "224.0.1.1:40456|interface=192.168.0.1"
    end

    test "parses IPC channel URI" do
      uri = "aeron:ipc"
      assert {:ok, parsed} = URI.parse(uri)
      assert parsed.transport == :ipc
    end

    test "roundtrips URI" do
      original = "aeron:udp?endpoint=224.0.1.1:40456"
      assert {:ok, parsed} = URI.parse(original)
      assert URI.to_string(parsed) == original
    end
  end

  describe "Frame encoding/decoding" do
    test "encodes and decodes data frame" do
      frame = %Frame{
        frame_length: 48,
        version: 0,
        flags: 0,
        type: :data,
        term_offset: 0,
        session_id: 1,
        stream_id: 10,
        term_id: 0,
        reserved_value: 0
      }

      encoded = Frame.encode(frame)
      assert {:ok, decoded} = Frame.decode(encoded)
      assert decoded.type == :data
      assert decoded.session_id == 1
      assert decoded.stream_id == 10
    end

    test "sets and checks fragment flags" do
      frame = %Frame{flags: 0}
      frame = Frame.set_begin_fragment(frame)
      assert Frame.begin_fragment?(frame)

      frame = Frame.set_end_fragment(frame)
      assert Frame.end_fragment?(frame)
    end

    test "creates data frame with payload" do
      payload = <<1, 2, 3, 4>>
      frame_data = Frame.data_frame(0, 1, 10, 0, payload)
      assert byte_size(frame_data) == Frame.frame_header_length() + 4
    end
  end
end
