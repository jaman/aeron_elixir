defmodule AeronElixir.LogBuffer.Descriptor do
  @moduledoc """
  Reads the log-buffer metadata region and derives publication geometry.

  A log file is laid out as three term partitions followed by a metadata region
  (`LogBufferDescriptor`):

      +--------+--------+--------+---------------+
      | Term 0 | Term 1 | Term 2 | Log Meta Data |
      +--------+--------+--------+---------------+

  The metadata region is `LOG_META_DATA_LENGTH` (4096) bytes. From its
  `initial_term_id`, `mtu_length`, `term_length` and `page_size` fields this module
  derives the same geometry the base `Publication` computes:

  * `term_buffer_length` = `term_length`
  * `max_message_length` = `min(term_length >> 3, 16 MiB)` (`FrameDescriptor.computeMaxMessageLength`)
  * `max_payload_length` = `mtu_length - HEADER_LENGTH` (32)
  * `position_bits_to_shift` = `log2(term_length)`
  * `max_possible_position` = `term_length * (1 <<< 31)`

  All field reads match the base `LogBufferDescriptor` offsets exactly.
  """

  @log_meta_data_length 4096
  @header_length 32
  @max_message_length 16 * 1024 * 1024

  @log_correlation_id_offset 256
  @log_initial_term_id_offset @log_correlation_id_offset + 8
  @log_default_frame_header_length_offset @log_initial_term_id_offset + 4
  @log_mtu_length_offset @log_default_frame_header_length_offset + 4
  @log_term_length_offset @log_mtu_length_offset + 4
  @log_page_size_offset @log_term_length_offset + 4

  @term_min_length 64 * 1024
  @term_max_length 1024 * 1024 * 1024
  @page_min_size 4 * 1024
  @page_max_size 1024 * 1024 * 1024

  defguardp is_power_of_two(value) when value > 0 and Bitwise.band(value, value - 1) == 0

  @type t :: %__MODULE__{
          correlation_id: integer(),
          initial_term_id: integer(),
          mtu_length: pos_integer(),
          term_buffer_length: pos_integer(),
          page_size: pos_integer(),
          max_message_length: pos_integer(),
          max_payload_length: pos_integer(),
          position_bits_to_shift: pos_integer(),
          max_possible_position: pos_integer()
        }

  defstruct [
    :correlation_id,
    :initial_term_id,
    :mtu_length,
    :term_buffer_length,
    :page_size,
    :max_message_length,
    :max_payload_length,
    :position_bits_to_shift,
    :max_possible_position
  ]

  @spec log_meta_data_length() :: 4096
  def log_meta_data_length, do: @log_meta_data_length

  @spec read(binary()) :: {:ok, t()} | {:error, term()}
  def read(metadata) when is_binary(metadata) and byte_size(metadata) >= @log_meta_data_length do
    correlation_id = read_i64(metadata, @log_correlation_id_offset)
    initial_term_id = read_i32(metadata, @log_initial_term_id_offset)
    mtu_length = read_i32(metadata, @log_mtu_length_offset)
    term_length = read_i32(metadata, @log_term_length_offset)
    page_size = read_i32(metadata, @log_page_size_offset)

    with :ok <- check_term_length(term_length),
         :ok <- check_page_size(page_size),
         {:ok, position_bits_to_shift} <- position_bits_to_shift(term_length) do
      {:ok,
       %__MODULE__{
         correlation_id: correlation_id,
         initial_term_id: initial_term_id,
         mtu_length: mtu_length,
         term_buffer_length: term_length,
         page_size: page_size,
         max_message_length: compute_max_message_length(term_length),
         max_payload_length: mtu_length - @header_length,
         position_bits_to_shift: position_bits_to_shift,
         max_possible_position: term_length * Bitwise.bsl(1, 31)
       }}
    end
  end

  def read(_), do: {:error, :metadata_too_short}

  @spec compute_max_message_length(pos_integer()) :: pos_integer()
  def compute_max_message_length(term_length) when is_integer(term_length) and term_length > 0 do
    min(Bitwise.bsr(term_length, 3), @max_message_length)
  end

  @spec position_bits_to_shift(pos_integer()) :: {:ok, pos_integer()} | {:error, term()}
  def position_bits_to_shift(term_length)
      when is_integer(term_length) and term_length >= @term_min_length and
             term_length <= @term_max_length and is_power_of_two(term_length),
      do: {:ok, trailing_zero_count(term_length)}

  def position_bits_to_shift(term_length),
    do: {:error, {:invalid_term_buffer_length, term_length}}

  defp check_term_length(term_length) when term_length < @term_min_length,
    do: {:error, {:term_length_too_short, term_length}}

  defp check_term_length(term_length) when term_length > @term_max_length,
    do: {:error, {:term_length_too_long, term_length}}

  defp check_term_length(term_length) when not is_power_of_two(term_length),
    do: {:error, {:term_length_not_power_of_two, term_length}}

  defp check_term_length(_term_length), do: :ok

  defp check_page_size(page_size) when page_size < @page_min_size,
    do: {:error, {:page_size_too_short, page_size}}

  defp check_page_size(page_size) when page_size > @page_max_size,
    do: {:error, {:page_size_too_long, page_size}}

  defp check_page_size(page_size) when not is_power_of_two(page_size),
    do: {:error, {:page_size_not_power_of_two, page_size}}

  defp check_page_size(_page_size), do: :ok

  defp trailing_zero_count(value), do: trailing_zero_count(value, 0)

  defp trailing_zero_count(value, count) when Bitwise.band(value, 1) == 0,
    do: trailing_zero_count(Bitwise.bsr(value, 1), count + 1)

  defp trailing_zero_count(_value, count), do: count

  defp read_i32(buffer, offset) do
    <<value::little-signed-32>> = binary_part(buffer, offset, 4)
    value
  end

  defp read_i64(buffer, offset) do
    <<value::little-signed-64>> = binary_part(buffer, offset, 8)
    value
  end
end
