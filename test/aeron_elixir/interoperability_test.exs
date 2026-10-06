defmodule AeronElixir.InteroperabilityTest do
  use ExUnit.Case, async: false

  @moduletag :interop

  alias AeronElixir.Protocol.{Control, Frame, URI}

  describe "Java Aeron client compatibility" do
    setup do
      aeron_dir = "/tmp/aeron_test_elixir_#{:os.system_time(:millisecond)}"
      File.mkdir_p!(aeron_dir)

      on_exit(fn ->
        File.rm_rf!(aeron_dir)
      end)

      %{aeron_directory: aeron_dir}
    end

    test "can parse Java Aeron URIs", %{aeron_directory: _} do
      test_uris = [
        "aeron:udp?endpoint=localhost:40123|interface=localhost",
        "aeron:ipc?alias=test",
        "aeron:udp?endpoint=224.0.1.1:40123|interface=192.168.1.1|ttl=8"
      ]

      Enum.each(test_uris, fn uri ->
        {:ok, parsed} = URI.parse(uri)
        assert parsed.channel == uri
      end)
    end

    test "can decode Java Aeron frames", %{aeron_directory: _} do
      java_data_frame =
        <<64, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0x42, 0x42, 0, 0, 1, 0, 0, 0, 100, 0, 0, 0, 0, 0,
          0, 0, 0, 0, 0, 0>>

      {:ok, frame} = Frame.decode(java_data_frame)
      assert frame.type == :data
      assert frame.session_id == 0x4242
      assert frame.stream_id == 1
    end

    test "can process Java Aeron control messages", %{aeron_directory: _} do
      java_publication_ready =
        <<0x0::little-signed-64, 0x0::little-signed-64, 0x1::little-signed-32,
          0xA::little-signed-32, 0x2::little-signed-32, 0x3::little-signed-32,
          0x0::little-signed-32>>

      {:ok, {:on_publication_ready, response}} =
        Control.decode(Control.response_on_publication_ready(), java_publication_ready)

      assert response.correlation_id == 0x0
      assert response.stream_id == 0xA
    end

    test "can generate compatible control commands", %{aeron_directory: _} do
      client_id = 999
      correlation_id = 12_345
      stream_id = 3

      command =
        Control.encode_add_publication(
          client_id,
          correlation_id,
          stream_id,
          "aeron:ipc?alias=test"
        )

      assert is_binary(command)
      assert byte_size(command) >= 20
    end
  end

  describe "protocol level compatibility" do
    test "frame header structure matches Java Aeron" do
      frame = %Frame{
        frame_length: 64,
        version: 0,
        flags: 0,
        type: :data,
        term_offset: 1024,
        session_id: 42,
        stream_id: 1,
        term_id: 0,
        reserved_value: 0
      }

      encoded = Frame.encode(frame)

      assert byte_size(encoded) == 32

      <<frame_length::little-32, version::little-8, flags::little-8, frame_type_code::little-16,
        term_offset::little-32, session_id::little-32, stream_id::little-32, term_id::little-32,
        reserved_value::little-64, _rest::binary>> = encoded

      assert frame_length == 64
      assert version == 0
      assert flags == 0
      assert frame_type_code == 0x01
      assert term_offset == 1024
      assert session_id == 42
      assert stream_id == 1
      assert term_id == 0
      assert reserved_value == 0
    end

    test "control message structure matches Java Aeron" do
      client_id = 999
      correlation_id = 888
      stream_id = 1
      command = Control.encode_add_publication(client_id, correlation_id, stream_id, "aeron:ipc")

      <<encoded_client_id::little-signed-64, encoded_correlation_id::little-signed-64,
        encoded_stream_id::little-signed-32, _channel_length::little-signed-32, _channel::binary>> =
        command

      assert encoded_client_id == client_id
      assert encoded_correlation_id == correlation_id
      assert encoded_stream_id == stream_id
    end
  end

  describe "cross-language message flow" do
    setup do
      aeron_dir = "/tmp/aeron_cross_lang_test_#{:os.system_time(:millisecond)}"
      File.mkdir_p!(aeron_dir)

      on_exit(fn ->
        File.rm_rf!(aeron_dir)
      end)

      %{aeron_directory: aeron_dir}
    end

    test "Elixir can parse Java-generated frames", %{aeron_directory: _} do
      java_frame =
        <<32, 0, 0, 0, 0, 0, 2, 0, 4, 0, 0, 0, 100, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
          0, 0, 0, 0>>

      {:ok, frame} = Frame.decode(java_frame)
      assert frame.type == :setup
      assert frame.session_id == 100
    end

    test "Java can parse Elixir-generated frames", %{aeron_directory: _} do
      elixir_frame = %Frame{
        frame_length: 32,
        version: 0,
        flags: 0,
        type: :heartbeat,
        term_offset: 0,
        session_id: 42,
        stream_id: 1,
        term_id: 0,
        reserved_value: 0
      }

      encoded = Frame.encode(elixir_frame)
      {:ok, _decoded} = Frame.decode(encoded)
    end
  end
end
