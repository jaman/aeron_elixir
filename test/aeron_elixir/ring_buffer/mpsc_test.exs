defmodule AeronElixir.RingBuffer.MPSCTest do
  use ExUnit.Case

  alias AeronElixir.NIF
  alias AeronElixir.RingBuffer.MPSC

  @capacity 1024
  @trailer_length 768
  @tail_position_offset 128
  @correlation_counter_offset 512

  setup do
    total = @capacity + @trailer_length
    path = "/tmp/aeron_mpsc_test_#{:erlang.unique_integer([:positive])}.bin"
    File.write!(path, :binary.copy(<<0>>, total))
    {:ok, region, base, ^total} = NIF.map_log(path)

    keep_mapped(region)
    on_exit(fn -> File.rm_rf!(path) end)

    {:ok, ring} = MPSC.new(base, total)
    %{ring: ring, base: base, total: total}
  end

  describe "new/2" do
    test "rejects a capacity that is not a power of two" do
      assert {:error, {:capacity_not_power_of_two, _}} = MPSC.new(0, 1000 + @trailer_length)
    end

    test "derives max_message_length as an eighth of capacity", %{ring: ring} do
      assert ring.max_message_length == div(@capacity, 8)
    end
  end

  describe "write/3" do
    test "writes a record in the Agrona ManyToOneRingBuffer layout", %{ring: ring, base: base} do
      message = "command-body"
      assert :ok = MPSC.write(ring, 0x01, message)

      record_length = 8 + byte_size(message)
      {:ok, length} = NIF.read_int32(base)
      {:ok, type} = NIF.read_int32(base + 4)
      {:ok, body} = NIF.read_binary(base + 8, byte_size(message), 0)

      assert length == record_length
      assert type == 0x01
      assert body == message
    end

    test "advances the tail by the 8-byte-aligned record length", %{ring: ring, base: base} do
      assert :ok = MPSC.write(ring, 0x01, "abc")

      {:ok, tail} = NIF.atomic_get_int64(base + @capacity + @tail_position_offset)
      assert tail == align(8 + 3, 8)
    end

    test "places successive records back to back", %{ring: ring, base: base} do
      assert :ok = MPSC.write(ring, 0x01, "first")
      first_aligned = align(8 + 5, 8)

      assert :ok = MPSC.write(ring, 0x04, "second-record")

      {:ok, length} = NIF.read_int32(base + first_aligned)
      {:ok, type} = NIF.read_int32(base + first_aligned + 4)
      {:ok, body} = NIF.read_binary(base + first_aligned + 8, byte_size("second-record"), 0)

      assert length == 8 + byte_size("second-record")
      assert type == 0x04
      assert body == "second-record"
    end

    test "rejects a message larger than max_message_length", %{ring: ring} do
      oversized = :binary.copy(<<0>>, ring.max_message_length + 1)
      assert {:error, :message_too_long} = MPSC.write(ring, 0x01, oversized)
    end

    test "reports back-pressure once the buffer is full", %{ring: ring} do
      payload = :binary.copy(<<0>>, ring.max_message_length)

      results =
        Stream.repeatedly(fn -> MPSC.write(ring, 0x01, payload) end)
        |> Enum.take(@capacity)

      assert :ok in results
      assert {:error, :buffer_full} in results
    end
  end

  describe "next_correlation_id/1" do
    test "returns a monotonic sequence from the correlation counter", %{ring: ring, base: base} do
      NIF.write_int64(base + @capacity + @correlation_counter_offset, 0)

      assert MPSC.next_correlation_id(ring) == 0
      assert MPSC.next_correlation_id(ring) == 1
      assert MPSC.next_correlation_id(ring) == 2
    end
  end

  defp align(value, alignment), do: Bitwise.band(value + alignment - 1, -alignment)

  defp keep_mapped(region), do: on_exit(fn -> region end)
end
