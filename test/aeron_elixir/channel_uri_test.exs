defmodule AeronElixir.ChannelUriTest do
  use ExUnit.Case, async: true

  alias AeronElixir.ChannelUri

  describe "ipc/1" do
    test "builds a bare IPC channel" do
      assert ChannelUri.ipc() == "aeron:ipc"
    end

    test "appends parameters in Aeron's key=value form" do
      assert ChannelUri.ipc(term_length: 65_536, alias: "orders") ==
               "aeron:ipc?term-length=65536|alias=orders"
    end
  end

  describe "udp/1" do
    test "requires an endpoint" do
      assert {:error, :endpoint_required} = ChannelUri.udp([])
    end

    test "builds a UDP channel with transport options" do
      assert ChannelUri.udp(
               endpoint: "localhost:40123",
               interface: "127.0.0.1",
               mtu: 1408,
               reliable: false,
               session_id: 42
             ) ==
               "aeron:udp?endpoint=localhost:40123|interface=127.0.0.1|mtu=1408|reliable=false|session-id=42"
    end

    test "rejects unknown parameters" do
      assert {:error, {:unknown_param, :bogus}} = ChannelUri.udp(endpoint: "h:1", bogus: 1)
    end

    test "rejects a term length that is not a power of two" do
      assert {:error, {:invalid_term_length, 1000}} =
               ChannelUri.udp(endpoint: "h:1", term_length: 1000)
    end
  end
end
