defmodule AeronElixir.NIFTest do
  use ExUnit.Case

  alias AeronElixir.NIF

  setup do
    path = "/tmp/aeron_nif_test_#{:erlang.unique_integer([:positive])}.bin"
    File.write!(path, :binary.copy(<<0>>, 4096))
    on_exit(fn -> File.rm_rf!(path) end)
    %{path: path}
  end

  describe "file mapping" do
    test "map_cnc returns a region reference and a base address", %{path: path} do
      assert {:ok, region, base} = NIF.map_cnc(path)
      keep_mapped(region)
      assert is_reference(region)
      assert is_integer(base)
    end

    test "map_log returns a region reference, the base address and the file length", %{path: path} do
      assert {:ok, region, base, length} = NIF.map_log(path)
      assert is_reference(region)
      assert is_integer(base)
      assert length == 4096
    end

    test "mapping raises the live region count", %{path: path} do
      before = NIF.mapped_region_count()
      {:ok, region, _base} = NIF.map_cnc(path)

      assert NIF.mapped_region_count() == before + 1
      assert is_reference(region)
    end

    test "map_cnc reports an error for a missing file" do
      assert {:error, _reason} =
               NIF.map_cnc("/tmp/does-not-exist-#{:erlang.unique_integer()}.bin")
    end
  end

  describe "integer accessors" do
    test "read_int64 reflects write_int64", %{path: path} do
      {:ok, region, base} = NIF.map_cnc(path)
      keep_mapped(region)
      {:ok, address} = NIF.get_buffer_address(base, 64)

      assert :ok = NIF.write_int64(address, -123_456_789)
      assert {:ok, -123_456_789} = NIF.read_int64(address)
    end

    test "read_int32 reflects write_int32", %{path: path} do
      {:ok, region, base} = NIF.map_cnc(path)
      keep_mapped(region)
      {:ok, address} = NIF.get_buffer_address(base, 0)

      assert :ok = NIF.write_int32(address, 1_000_000)
      assert {:ok, 1_000_000} = NIF.read_int32(address)
    end

    test "write_int64_ordered is visible to atomic_get_int64", %{path: path} do
      {:ok, region, base} = NIF.map_cnc(path)
      keep_mapped(region)
      {:ok, address} = NIF.get_buffer_address(base, 128)

      assert :ok = NIF.write_int64_ordered(address, 42)
      assert {:ok, 42} = NIF.atomic_get_int64(address)
    end
  end

  describe "atomic operations" do
    test "atomic_fetch_add_int64 returns the previous value and applies the delta", %{path: path} do
      {:ok, region, base} = NIF.map_cnc(path)
      keep_mapped(region)
      {:ok, address} = NIF.get_buffer_address(base, 256)

      NIF.write_int64(address, 100)
      assert {:ok, 100} = NIF.atomic_fetch_add_int64(address, 25)
      assert {:ok, 125} = NIF.atomic_get_int64(address)
    end

    test "atomic_cas_int64 succeeds when the witness matches", %{path: path} do
      {:ok, region, base} = NIF.map_cnc(path)
      keep_mapped(region)
      {:ok, address} = NIF.get_buffer_address(base, 320)

      NIF.write_int64(address, 7)
      assert {:ok, 7} = NIF.atomic_cas_int64(address, 7, 99)
      assert {:ok, 99} = NIF.atomic_get_int64(address)
    end

    test "atomic_cas_int64 fails and reports the current value when the witness differs", %{
      path: path
    } do
      {:ok, region, base} = NIF.map_cnc(path)
      keep_mapped(region)
      {:ok, address} = NIF.get_buffer_address(base, 384)

      NIF.write_int64(address, 7)
      assert {:failed, 7} = NIF.atomic_cas_int64(address, 1, 99)
      assert {:ok, 7} = NIF.atomic_get_int64(address)
    end
  end

  describe "binary accessors" do
    test "read_binary reflects write_binary at an offset", %{path: path} do
      {:ok, region, base} = NIF.map_cnc(path)
      keep_mapped(region)
      {:ok, address} = NIF.get_buffer_address(base, 0)

      assert :ok = NIF.write_binary(address, 512, "hello aeron")
      assert {:ok, "hello aeron"} = NIF.read_binary(address, 11, 512)
    end
  end

  defp keep_mapped(region), do: on_exit(fn -> region end)
end
