defmodule AeronElixir.Protocol.FrameTest do
  use ExUnit.Case

  alias AeronElixir.Protocol.Frame

  describe "encode/1" do
    test "encodes data frame" do
      frame = %Frame{
        frame_length: 56,
        version: 0,
        flags: 0,
        type: :data,
        term_offset: 1024,
        session_id: 1,
        stream_id: 10,
        term_id: 100,
        reserved_value: 0
      }

      encoded = Frame.encode(frame)
      assert is_binary(encoded)
      assert byte_size(encoded) == 32
    end

    test "encodes setup frame" do
      frame = %Frame{
        frame_length: 32,
        version: 0,
        flags: 0,
        type: :setup,
        term_offset: 0,
        session_id: 1,
        stream_id: 10,
        term_id: 100,
        reserved_value: 0
      }

      encoded = Frame.encode(frame)
      assert is_binary(encoded)
      assert byte_size(encoded) == 32
    end
  end

  describe "decode/1" do
    test "decodes data frame" do
      frame = %Frame{
        frame_length: 56,
        version: 0,
        flags: 0,
        type: :data,
        term_offset: 0,
        session_id: 1,
        stream_id: 10,
        term_id: 100,
        reserved_value: 0
      }

      header = Frame.encode(frame)

      {:ok, decoded_frame} = Frame.decode(header)
      assert decoded_frame.type == :data
      assert decoded_frame.session_id == 1
      assert decoded_frame.stream_id == 10
    end

    test "returns error for malformed frame" do
      malformed = <<1, 2, 3>>
      assert {:error, :invalid_frame} = Frame.decode(malformed)
    end
  end
end
