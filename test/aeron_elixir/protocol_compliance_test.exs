defmodule AeronElixir.ProtocolComplianceTest do
  use ExUnit.Case

  @moduletag :protocol

  alias AeronElixir.Protocol.{Control, Frame, URI}

  describe "frame header compliance" do
    test "frame header has correct length" do
      frame = %Frame{
        frame_length: 32,
        version: 0,
        flags: 0,
        type: :data,
        term_offset: 0,
        session_id: 0,
        stream_id: 0,
        term_id: 0,
        reserved_value: 0
      }

      encoded = Frame.encode(frame)
      assert byte_size(encoded) == 32
    end

    test "frame header uses little-endian byte order" do
      frame = %Frame{
        frame_length: 1000,
        version: 0,
        flags: 0,
        type: :data,
        term_offset: 500,
        session_id: 999,
        stream_id: 777,
        term_id: 123,
        reserved_value: 0
      }

      encoded = Frame.encode(frame)

      <<frame_length::little-32, _version::little-8, _flags::little-8, _type::little-16,
        term_offset::little-32, session_id::little-32, stream_id::little-32, term_id::little-32,
        _reserved::little-64>> = encoded

      assert frame_length == 1000
      assert term_offset == 500
      assert session_id == 999
      assert stream_id == 777
      assert term_id == 123
    end

    test "frame type codes match specification" do
      expected_codes = [
        {0x00, :pad},
        {0x01, :data},
        {0x02, :setup},
        {0x03, :nak},
        {0x04, :status_message},
        {0x05, :rrt},
        {0x06, :heartbeat}
      ]

      Enum.each(expected_codes, fn {expected_code, type} ->
        frame = %Frame{
          frame_length: 32,
          version: 0,
          flags: 0,
          type: type,
          term_offset: 0,
          session_id: 0,
          stream_id: 0,
          term_id: 0,
          reserved_value: 0
        }

        encoded = Frame.encode(frame)
        <<_::little-48, frame_type_code::little-16, _rest::binary>> = encoded

        assert frame_type_code == expected_code,
               "Frame type #{type} should have code #{expected_code}, got #{frame_type_code}"
      end)
    end
  end

  describe "fragmentation compliance" do
    test "begin fragment flag has correct value" do
      assert Frame.flag_begin_fragment() == 0x80
    end

    test "end fragment flag has correct value" do
      assert Frame.flag_end_fragment() == 0x40
    end

    test "fragment flags can be combined" do
      begin_and_end = Bitwise.bor(Frame.flag_begin_fragment(), Frame.flag_end_fragment())
      assert begin_and_end == 0xC0
    end
  end

  describe "control protocol compliance" do
    test "add_publication body lays out client_id/correlation_id/stream_id/channel" do
      client_id = 1
      correlation_id = 123_456_789
      stream_id = 7
      channel = "aeron:ipc"
      command = Control.encode_add_publication(client_id, correlation_id, stream_id, channel)

      <<encoded_client_id::little-signed-64, encoded_correlation_id::little-signed-64,
        encoded_stream_id::little-signed-32, channel_length::little-signed-32,
        encoded_channel::binary>> = command

      assert encoded_client_id == client_id
      assert encoded_correlation_id == correlation_id
      assert encoded_stream_id == stream_id
      assert channel_length == byte_size(channel)
      assert encoded_channel == channel
    end

    test "add_subscription body carries registration correlation id slot" do
      client_id = 1
      correlation_id = 123
      registration_id = Control.registration_id_new()
      stream_id = 7
      channel = "aeron:ipc"

      command =
        Control.encode_add_subscription(
          client_id,
          correlation_id,
          registration_id,
          stream_id,
          channel
        )

      <<encoded_client_id::little-signed-64, encoded_correlation_id::little-signed-64,
        encoded_registration_id::little-signed-64, encoded_stream_id::little-signed-32,
        channel_length::little-signed-32, encoded_channel::binary>> = command

      assert encoded_client_id == client_id
      assert encoded_correlation_id == correlation_id
      assert encoded_registration_id == -1
      assert encoded_stream_id == stream_id
      assert channel_length == byte_size(channel)
      assert encoded_channel == channel
    end

    test "command IDs match specification" do
      assert Control.command_add_publication() == 0x01
      assert Control.command_remove_publication() == 0x02
      assert Control.command_add_subscription() == 0x04
      assert Control.command_remove_subscription() == 0x05
      assert Control.command_client_keepalive() == 0x06
      assert Control.command_client_close() == 0x0B
    end

    test "response type IDs match specification" do
      assert Control.response_on_error() == 0x0F01
      assert Control.response_on_available_image() == 0x0F02
      assert Control.response_on_publication_ready() == 0x0F03
      assert Control.response_on_operation_success() == 0x0F04
      assert Control.response_on_subscription_ready() == 0x0F07
    end

    test "decode dispatches on broadcast message type id" do
      correlation_id = 999

      publication_ready =
        <<correlation_id::little-signed-64, 1::little-signed-64, 1::little-signed-32,
          2::little-signed-32, 3::little-signed-32, 4::little-signed-32, 0::little-signed-32>>

      {:ok, {:on_publication_ready, decoded}} =
        Control.decode(Control.response_on_publication_ready(), publication_ready)

      assert decoded.correlation_id == correlation_id
      assert decoded.publication_limit_counter_id == 3
      assert decoded.channel_status_indicator_id == 4

      error =
        <<correlation_id::little-signed-64, 10::little-signed-32, 5::little-signed-32, "Error">>

      {:ok, {:on_error, error_decoded}} = Control.decode(Control.response_on_error(), error)
      assert error_decoded.offending_command_correlation_id == correlation_id
      assert error_decoded.error_code == 10
      assert error_decoded.error_message == "Error"

      subscription_ready =
        <<correlation_id::little-signed-64, 2::little-signed-32>>

      {:ok, {:on_subscription_ready, sub_decoded}} =
        Control.decode(Control.response_on_subscription_ready(), subscription_ready)

      assert sub_decoded.correlation_id == correlation_id
      assert sub_decoded.channel_status_indicator_id == 2

      operation_success = <<correlation_id::little-signed-64>>

      {:ok, {:on_operation_success, op_decoded}} =
        Control.decode(Control.response_on_operation_success(), operation_success)

      assert op_decoded.correlation_id == correlation_id
    end
  end

  describe "URI compliance" do
    test "URI scheme must be 'aeron'" do
      {:ok, uri} = URI.parse("aeron:ipc")
      assert uri.channel == "aeron:ipc"

      assert {:error, :invalid_uri} = URI.parse("not-aeron:ipc")
    end

    test "IPC URIs have correct format" do
      test_cases = [
        "aeron:ipc",
        "aeron:ipc?alias=test",
        "aeron:ipc?term-length=65536"
      ]

      Enum.each(test_cases, fn uri_string ->
        {:ok, uri} = URI.parse(uri_string)
        assert uri.transport == :ipc
      end)
    end

    test "UDP URIs have correct format" do
      test_cases = [
        "aeron:udp?endpoint=localhost:40123",
        "aeron:udp?endpoint=224.0.1.1:40123|interface=192.168.1.1",
        "aeron:udp?endpoint=localhost:40123|ttl=8|mtu=1408"
      ]

      Enum.each(test_cases, fn uri_string ->
        {:ok, uri} = URI.parse(uri_string)
        assert uri.transport == :udp
        assert uri.endpoint =~ ~r/:\d+/
      end)
    end
  end

  describe "buffer and memory layout compliance" do
    test "MTU values respect alignment requirements" do
      standard_mtus = [1408, 4096, 8192, 16_384]

      Enum.each(standard_mtus, fn mtu ->
        assert rem(mtu, 32) == 0, "MTU #{mtu} must be 32-byte aligned"
      end)
    end

    test "term buffer sizes are powers of 2" do
      standard_terms = [65_536, 131_072, 262_144, 1_048_576]

      Enum.each(standard_terms, fn term_size ->
        assert Bitwise.band(term_size, term_size - 1) == 0,
               "Term size #{term_size} must be a power of 2"
      end)
    end
  end
end
