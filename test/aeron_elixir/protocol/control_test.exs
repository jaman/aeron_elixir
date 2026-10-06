defmodule AeronElixir.Protocol.ControlTest do
  use ExUnit.Case

  alias AeronElixir.Protocol.Control

  defp encode_ascii(value), do: <<byte_size(value)::little-signed-32>> <> value

  defp align_padding(length, alignment) do
    :binary.copy(<<0>>, rem(alignment - rem(length, alignment), alignment))
  end

  describe "encode_add_exclusive_publication/4" do
    test "uses the exclusive command id with the PublicationMessageFlyweight layout" do
      assert Control.command_add_exclusive_publication() == 0x03

      assert Control.encode_add_exclusive_publication(7, 9, 1001, "aeron:ipc") ==
               Control.encode_add_publication(7, 9, 1001, "aeron:ipc")
    end
  end

  describe "decode/2 for on_unavailable_image" do
    test "matches ImageMessageFlyweight layout" do
      body =
        <<77::little-signed-64, 55::little-signed-64, 1001::little-signed-32>> <>
          encode_ascii("aeron:ipc")

      assert {:ok,
              {:on_unavailable_image,
               %{
                 correlation_id: 77,
                 subscription_registration_id: 55,
                 stream_id: 1001,
                 channel: "aeron:ipc"
               }}} = Control.decode(Control.response_on_unavailable_image(), body)
    end
  end

  describe "encode_add_publication/4" do
    test "matches PublicationMessageFlyweight layout" do
      channel = "aeron:udp?endpoint=localhost:40123"
      encoded = Control.encode_add_publication(7, 42, 10, channel)

      assert encoded ==
               <<7::little-signed-64, 42::little-signed-64, 10::little-signed-32>> <>
                 encode_ascii(channel)

      assert byte_size(encoded) == 20 + 4 + byte_size(channel)
    end
  end

  describe "encode_add_subscription/5" do
    test "matches SubscriptionMessageFlyweight layout with registration slot" do
      channel = "aeron:ipc"
      registration_id = Control.registration_id_new()
      encoded = Control.encode_add_subscription(7, 42, registration_id, 10, channel)

      assert encoded ==
               <<7::little-signed-64, 42::little-signed-64, -1::little-signed-64,
                 10::little-signed-32>> <> encode_ascii(channel)
    end

    test "carries an existing registration correlation id" do
      encoded = Control.encode_add_subscription(7, 42, 555, 10, "aeron:ipc")

      <<_client::little-signed-64, _corr::little-signed-64, registration_id::little-signed-64,
        _stream::little-signed-32, _rest::binary>> = encoded

      assert registration_id == 555
    end
  end

  describe "encode_client_keepalive/2 and encode_client_close/2" do
    test "match CorrelatedMessageFlyweight layout" do
      assert Control.encode_client_keepalive(7, 42) ==
               <<7::little-signed-64, 42::little-signed-64>>

      assert Control.encode_client_close(7, 42) ==
               <<7::little-signed-64, 42::little-signed-64>>
    end
  end

  describe "decode_publication_ready/1 round-trip" do
    test "decodes all PublicationBuffersReadyFlyweight fields" do
      log_file_name = "/dev/shm/aeron/logs/publication-1.log"

      body =
        <<99::little-signed-64, 1000::little-signed-64, 5::little-signed-32, 10::little-signed-32,
          3::little-signed-32, 4::little-signed-32>> <> encode_ascii(log_file_name)

      assert {:ok, decoded} = Control.decode_publication_ready(body)

      assert decoded == %{
               correlation_id: 99,
               registration_id: 1000,
               session_id: 5,
               stream_id: 10,
               publication_limit_counter_id: 3,
               channel_status_indicator_id: 4,
               log_file_name: log_file_name
             }
    end

    test "dispatched through decode/2 with the publication ready type id" do
      body =
        <<99::little-signed-64, 1000::little-signed-64, 5::little-signed-32, 10::little-signed-32,
          3::little-signed-32, 4::little-signed-32>> <> encode_ascii("log")

      assert {:ok, {:on_publication_ready, decoded}} =
               Control.decode(Control.response_on_publication_ready(), body)

      assert decoded.session_id == 5
    end

    test "rejects truncated bodies" do
      assert {:error, :invalid_publication_ready} =
               Control.decode_publication_ready(<<1::little-signed-64>>)
    end
  end

  describe "decode_subscription_ready/1 round-trip" do
    test "decodes SubscriptionReadyFlyweight fields" do
      body = <<77::little-signed-64, 2::little-signed-32>>

      assert {:ok, %{correlation_id: 77, channel_status_indicator_id: 2}} =
               Control.decode_subscription_ready(body)
    end

    test "rejects truncated bodies" do
      assert {:error, :invalid_subscription_ready} =
               Control.decode_subscription_ready(<<1::little-signed-32>>)
    end
  end

  describe "decode_available_image/1 round-trip" do
    test "decodes ImageBuffersReadyFlyweight including aligned source identity" do
      log_file_name = "/dev/shm/aeron/logs/image-1.log"
      source_identity = "127.0.0.1:54321"

      body =
        <<99::little-signed-64, 5::little-signed-32, 10::little-signed-32, 1000::little-signed-64,
          3::little-signed-32>> <>
          encode_ascii(log_file_name) <>
          align_padding(byte_size(log_file_name), 4) <>
          encode_ascii(source_identity)

      assert {:ok, decoded} = Control.decode_available_image(body)

      assert decoded == %{
               correlation_id: 99,
               session_id: 5,
               stream_id: 10,
               subscription_registration_id: 1000,
               subscriber_position_id: 3,
               log_file_name: log_file_name,
               source_identity: source_identity
             }
    end

    test "handles a log file name whose length is already 4-aligned" do
      log_file_name = "abcd"
      source_identity = "src"

      body =
        <<99::little-signed-64, 5::little-signed-32, 10::little-signed-32, 1000::little-signed-64,
          3::little-signed-32>> <>
          encode_ascii(log_file_name) <>
          align_padding(byte_size(log_file_name), 4) <>
          encode_ascii(source_identity)

      assert {:ok, decoded} = Control.decode_available_image(body)
      assert decoded.log_file_name == log_file_name
      assert decoded.source_identity == source_identity
    end
  end

  describe "decode_error/1 round-trip" do
    test "decodes ErrorResponseFlyweight fields" do
      message = "channel unknown"

      body =
        <<48::little-signed-64, 10::little-signed-32>> <> encode_ascii(message)

      assert {:ok, decoded} = Control.decode_error(body)

      assert decoded == %{
               offending_command_correlation_id: 48,
               error_code: 10,
               error_message: message
             }
    end

    test "rejects truncated bodies" do
      assert {:error, :invalid_error_response} = Control.decode_error(<<1::little-signed-32>>)
    end
  end

  describe "decode/2 dispatch" do
    test "returns an error tuple for an unknown response type id" do
      assert {:error, {:unknown_response_type, 0xABCD}} = Control.decode(0xABCD, <<>>)
    end
  end

  describe "destination commands" do
    test "command ids match the base ControlProtocolEvents" do
      assert Control.command_add_destination() == 0x07
      assert Control.command_remove_destination() == 0x08
      assert Control.command_add_rcv_destination() == 0x0C
      assert Control.command_remove_rcv_destination() == 0x0D
    end

    test "encode_destination lays out the DestinationMessageFlyweight" do
      encoded = Control.encode_destination(11, 22, 33, "aeron:udp?endpoint=127.0.0.1:40123")

      assert <<client_id::little-signed-64, correlation_id::little-signed-64,
               registration_correlation_id::little-signed-64, length::little-signed-32,
               channel::binary-size(34)>> = encoded

      assert client_id == 11
      assert correlation_id == 22
      assert registration_correlation_id == 33
      assert length == 34
      assert channel == "aeron:udp?endpoint=127.0.0.1:40123"
    end
  end
end
