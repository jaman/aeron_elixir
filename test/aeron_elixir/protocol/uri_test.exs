defmodule AeronElixir.Protocol.URITest do
  use ExUnit.Case

  doctest AeronElixir.Protocol.URI

  alias AeronElixir.Protocol.URI

  describe "parse/1" do
    test "parses UDP channel" do
      {:ok, uri} = URI.parse("aeron:udp?endpoint=localhost:40123|interface=localhost")
      assert uri.transport == :udp
      assert uri.endpoint == "localhost:40123|interface=localhost"
    end

    test "parses IPC channel" do
      {:ok, uri} = URI.parse("aeron:ipc")
      assert uri.transport == :ipc
    end

    test "parses with stream ID" do
      {:ok, uri} = URI.parse("aeron:udp?endpoint=localhost:40123|stream-id=100")
      assert uri.transport == :udp
      assert uri.endpoint == "localhost:40123|stream-id=100"
    end

    test "handles invalid URI format" do
      assert {:error, :invalid_uri} = URI.parse("invalid")
    end
  end

  describe "to_string/1" do
    test "converts parsed URI back to string" do
      original = "aeron:udp?endpoint=localhost:40123|stream-id=100"
      {:ok, uri} = URI.parse(original)
      assert URI.to_string(uri) == original
    end
  end
end
