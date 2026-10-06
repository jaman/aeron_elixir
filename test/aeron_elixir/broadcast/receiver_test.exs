defmodule AeronElixir.Broadcast.ReceiverTest do
  use ExUnit.Case, async: false

  alias AeronElixir.Broadcast.Receiver
  alias AeronElixir.NIF

  @capacity 1024
  @trailer_length 128
  @record_header_length 8
  @padding_msg_type_id -1

  setup do
    path = Path.join(System.tmp_dir!(), "broadcast-#{:erlang.unique_integer([:positive])}.dat")
    File.write!(path, :binary.copy(<<0>>, @capacity + @trailer_length))

    {:ok, region, base, _length} = NIF.map_log(path)
    Process.put(:mapped_region, region)
    on_exit(fn -> File.rm(path) end)

    %{base: base, path: path}
  end

  defp receiver(base), do: Receiver.new(base, @capacity + @trailer_length)

  defp put_record(base, offset, type_id, payload) do
    length = @record_header_length + byte_size(payload)
    :ok = NIF.write_int32(base + offset, length)
    :ok = NIF.write_int32(base + offset + 4, type_id)
    :ok = NIF.write_binary(base + offset + @record_header_length, 0, payload)
    offset + align(length)
  end

  defp put_raw_length(base, offset, length, type_id) do
    :ok = NIF.write_int32(base + offset, length)
    :ok = NIF.write_int32(base + offset + 4, type_id)
  end

  defp put_counters(base, tail_intent, tail, latest) do
    :ok = NIF.write_int64(base + @capacity, tail_intent)
    :ok = NIF.write_int64(base + @capacity + 8, tail)
    :ok = NIF.write_int64(base + @capacity + 16, latest)
  end

  defp align(value), do: Bitwise.band(value + 7, -8)

  describe "receive/3 against live mapped memory" do
    test "reads a single record and reports the next position", %{base: base} do
      next = put_record(base, 0, 7, "alpha")
      put_counters(base, next, next, 0)

      assert {:ok, [{7, "alpha"}], ^next} = Receiver.receive(receiver(base), 0)
    end

    test "reads several records in order", %{base: base} do
      offset = put_record(base, 0, 7, "alpha")
      offset = put_record(base, offset, 9, "bravo")
      next = put_record(base, offset, 11, "charlie")
      put_counters(base, next, next, 0)

      assert {:ok, [{7, "alpha"}, {9, "bravo"}, {11, "charlie"}], ^next} =
               Receiver.receive(receiver(base), 0)
    end

    test "honours the record limit and resumes from the returned position", %{base: base} do
      offset = put_record(base, 0, 7, "alpha")
      offset = put_record(base, offset, 9, "bravo")
      next = put_record(base, offset, 11, "charlie")
      put_counters(base, next, next, 0)

      assert {:ok, [{7, "alpha"}, {9, "bravo"}], position} =
               Receiver.receive(receiver(base), 0, 2)

      assert {:ok, [{11, "charlie"}], ^next} = Receiver.receive(receiver(base), position, 2)
    end

    test "returns no records when the cursor is at the tail", %{base: base} do
      next = put_record(base, 0, 7, "alpha")
      put_counters(base, next, next, 0)

      assert {:ok, [], ^next} = Receiver.receive(receiver(base), next)
    end

    test "reads an empty payload", %{base: base} do
      next = put_record(base, 0, 7, "")
      put_counters(base, next, next, 0)

      assert {:ok, [{7, ""}], ^next} = Receiver.receive(receiver(base), 0)
    end

    test "follows a padding record to the start of the buffer", %{base: base} do
      padding_offset = @capacity - 16
      put_raw_length(base, padding_offset, 16, @padding_msg_type_id)
      put_record(base, 0, 9, "wrapped")

      tail = padding_offset + 16 + align(@record_header_length + 7)
      put_counters(base, tail, tail, padding_offset)

      assert {:ok, [{9, "wrapped"}], ^tail} = Receiver.receive(receiver(base), padding_offset)
    end
  end

  describe "receive/3 rejects unusable records" do
    test "a negative record length is reported as lapped, not raised", %{base: base} do
      put_raw_length(base, 0, -1_881_818_289, 7)
      put_counters(base, 64, 64, 0)

      assert {:error, :lapped} = Receiver.receive(receiver(base), 0)
    end

    test "a record length below the header is reported as lapped", %{base: base} do
      put_raw_length(base, 0, 4, 7)
      put_counters(base, 64, 64, 0)

      assert {:error, :lapped} = Receiver.receive(receiver(base), 0)
    end

    test "a record extending past capacity is reported as lapped", %{base: base} do
      put_raw_length(base, 0, @capacity + 64, 7)
      put_counters(base, 64, 64, 0)

      assert {:error, :lapped} = Receiver.receive(receiver(base), 0)
    end

    test "a cursor the transmitter has overrun is reported as lapped", %{base: base} do
      next = put_record(base, 0, 7, "alpha")
      put_counters(base, @capacity + next, next, next)

      assert {:error, :lapped} = Receiver.receive(receiver(base), 0)
    end
  end

  describe "latest_position/1" do
    test "reads the latest counter from the trailer", %{base: base} do
      put_counters(base, 512, 512, 480)

      assert Receiver.latest_position(receiver(base)) == 480
    end
  end
end
