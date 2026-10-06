defmodule AeronElixir.LogBuffer.DescriptorTest do
  use ExUnit.Case

  alias AeronElixir.LogBuffer.Descriptor

  @log_correlation_id_offset 256
  @log_initial_term_id_offset @log_correlation_id_offset + 8
  @log_default_frame_header_length_offset @log_initial_term_id_offset + 4
  @log_mtu_length_offset @log_default_frame_header_length_offset + 4
  @log_term_length_offset @log_mtu_length_offset + 4
  @log_page_size_offset @log_term_length_offset + 4

  defp put_i32(buffer, offset, value) do
    <<before::binary-size(^offset), _::binary-size(4), rest::binary>> = buffer
    before <> <<value::little-signed-32>> <> rest
  end

  defp put_i64(buffer, offset, value) do
    <<before::binary-size(^offset), _::binary-size(8), rest::binary>> = buffer
    before <> <<value::little-signed-64>> <> rest
  end

  defp build_metadata(opts) do
    :binary.copy(<<0>>, Descriptor.log_meta_data_length())
    |> put_i64(@log_correlation_id_offset, Keyword.fetch!(opts, :correlation_id))
    |> put_i32(@log_initial_term_id_offset, Keyword.fetch!(opts, :initial_term_id))
    |> put_i32(@log_mtu_length_offset, Keyword.fetch!(opts, :mtu_length))
    |> put_i32(@log_term_length_offset, Keyword.fetch!(opts, :term_length))
    |> put_i32(@log_page_size_offset, Keyword.fetch!(opts, :page_size))
  end

  describe "read/1" do
    test "reads metadata fields and derives publication geometry" do
      metadata =
        build_metadata(
          correlation_id: 987_654_321,
          initial_term_id: -42,
          mtu_length: 1408,
          term_length: 1_048_576,
          page_size: 4096
        )

      assert {:ok, descriptor} = Descriptor.read(metadata)

      assert descriptor.correlation_id == 987_654_321
      assert descriptor.initial_term_id == -42
      assert descriptor.mtu_length == 1408
      assert descriptor.term_buffer_length == 1_048_576
      assert descriptor.page_size == 4096
      assert descriptor.max_payload_length == 1408 - 32
      assert descriptor.max_message_length == div(1_048_576, 8)
      assert descriptor.position_bits_to_shift == 20
      assert descriptor.max_possible_position == 1_048_576 * Bitwise.bsl(1, 31)
    end

    test "max_message_length is capped at 16 MiB for large terms" do
      metadata =
        build_metadata(
          correlation_id: 1,
          initial_term_id: 0,
          mtu_length: 8192,
          term_length: 1_073_741_824,
          page_size: 4096
        )

      assert {:ok, descriptor} = Descriptor.read(metadata)
      assert descriptor.max_message_length == 16 * 1024 * 1024
      assert descriptor.position_bits_to_shift == 30
    end

    test "rejects a term length that is not a power of two" do
      metadata =
        build_metadata(
          correlation_id: 1,
          initial_term_id: 0,
          mtu_length: 1408,
          term_length: 100_000,
          page_size: 4096
        )

      assert {:error, {:term_length_not_power_of_two, 100_000}} = Descriptor.read(metadata)
    end

    test "rejects a term length below the minimum" do
      metadata =
        build_metadata(
          correlation_id: 1,
          initial_term_id: 0,
          mtu_length: 1408,
          term_length: 32_768,
          page_size: 4096
        )

      assert {:error, {:term_length_too_short, 32_768}} = Descriptor.read(metadata)
    end

    test "rejects a page size that is not a power of two" do
      metadata =
        build_metadata(
          correlation_id: 1,
          initial_term_id: 0,
          mtu_length: 1408,
          term_length: 65_536,
          page_size: 5000
        )

      assert {:error, {:page_size_not_power_of_two, 5000}} = Descriptor.read(metadata)
    end

    test "rejects a metadata region that is too short" do
      assert {:error, :metadata_too_short} = Descriptor.read(<<0, 0, 0>>)
    end
  end

  describe "position_bits_to_shift/1" do
    test "matches the base LogBufferDescriptor table" do
      table = [
        {65_536, 16},
        {131_072, 17},
        {262_144, 18},
        {524_288, 19},
        {1_048_576, 20},
        {2_097_152, 21},
        {4_194_304, 22},
        {1_073_741_824, 30}
      ]

      for {term_length, expected} <- table do
        assert {:ok, ^expected} = Descriptor.position_bits_to_shift(term_length)
      end
    end

    test "rejects an out-of-range term length" do
      assert {:error, {:invalid_term_buffer_length, 1024}} =
               Descriptor.position_bits_to_shift(1024)
    end
  end

  describe "compute_max_message_length/1" do
    test "matches FrameDescriptor.computeMaxMessageLength" do
      assert Descriptor.compute_max_message_length(65_536) == 8192
      assert Descriptor.compute_max_message_length(1_048_576) == 131_072
      assert Descriptor.compute_max_message_length(1_073_741_824) == 16 * 1024 * 1024
    end
  end
end
