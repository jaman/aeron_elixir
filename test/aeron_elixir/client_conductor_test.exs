defmodule AeronElixir.ClientConductorTest do
  use ExUnit.Case, async: false

  alias AeronElixir.Broadcast.Receiver
  alias AeronElixir.ClientConductor
  alias AeronElixir.NIF

  @capacity 256
  @trailer_length 128

  setup do
    path = Path.join(System.tmp_dir!(), "conductor-#{:erlang.unique_integer([:positive])}.dat")
    File.write!(path, :binary.copy(<<0>>, @capacity + @trailer_length))

    {:ok, region, base, _length} = NIF.map_log(path)
    Process.put(:mapped_region, region)
    on_exit(fn -> File.rm(path) end)

    %{base: base}
  end

  defp receiver(base, tail_intent, tail, latest) do
    :ok = NIF.write_int64(base + @capacity, tail_intent)
    :ok = NIF.write_int64(base + @capacity + 8, tail)
    :ok = NIF.write_int64(base + @capacity + 16, latest)

    Receiver.new(base, @capacity + @trailer_length)
  end

  describe "next_broadcast_position/2" do
    test "resumes a completed drain at the position it reached", %{base: base} do
      receiver = receiver(base, 4096, 4096, 4096)

      assert ClientConductor.next_broadcast_position({:ok, [], 512}, receiver) == 512
      assert ClientConductor.next_broadcast_position({:ok, [{7, "x"}], 640}, receiver) == 640
    end

    test "resumes a lapped drain at the transmitter's latest record", %{base: base} do
      receiver = receiver(base, 8192, 8192, 7936)

      assert Receiver.latest_position(receiver) == 7936
      assert ClientConductor.next_broadcast_position({:error, :lapped}, receiver) == 7936
    end

    test "a lapped drain moves the position forward rather than leaving it stale", %{base: base} do
      stale_position = 128
      receiver = receiver(base, 8192, 8192, 7936)

      resumed = ClientConductor.next_broadcast_position({:error, :lapped}, receiver)

      assert resumed > stale_position
      refute resumed == stale_position
    end
  end
end
