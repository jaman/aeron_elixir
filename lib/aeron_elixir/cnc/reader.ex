defmodule AeronElixir.CnC.Reader do
  @moduledoc """
  Reads and manages the Aeron CnC (Command and Control) file.

  Maps `cnc.dat` through the NIF, reads the metadata region and resolves the
  absolute base addresses of the five buffers it frames: `to_driver`, `to_clients`,
  the counter metadata/values regions and the error log. The to-driver ring buffer
  is exposed as a live `RingBuffer.MPSC` writer; the to-clients broadcast buffer is
  exposed as a live `Broadcast.Receiver` reading the mapped memory directly.
  All protocol logic stays in pure Elixir; only the raw memory access is native.
  """

  alias AeronElixir.Broadcast.Receiver
  alias AeronElixir.NIF
  alias AeronElixir.RingBuffer.MPSC

  @cnc_version_and_meta_data_length 128
  @cnc_version_major 0
  @await_interval_ms 16

  @type t :: %__MODULE__{
          region: reference(),
          cnc_file: integer(),
          metadata: map(),
          to_driver: MPSC.t(),
          to_clients_address: integer(),
          to_clients_length: pos_integer(),
          counters_metadata_address: integer(),
          counters_values_address: integer(),
          error_log_address: integer()
        }

  defstruct [
    :region,
    :cnc_file,
    :metadata,
    :to_driver,
    :to_clients_address,
    :to_clients_length,
    :counters_metadata_address,
    :counters_values_address,
    :error_log_address
  ]

  @spec connect(String.t(), pos_integer()) :: {:ok, t()} | {:error, term()}
  def connect(aeron_directory, _timeout_ms \\ 5000) do
    cnc_path = Path.join(aeron_directory, "cnc.dat")

    with {:ok, region, cnc_file} <- NIF.map_cnc(cnc_path),
         {:ok, metadata} <- NIF.read_metadata(cnc_file),
         :ok <- verify_version(metadata.cnc_version),
         {:ok, addresses} <- resolve_addresses(cnc_file, metadata),
         {:ok, to_driver} <- MPSC.new(addresses.to_driver, metadata.to_driver_buffer_length) do
      {:ok,
       %__MODULE__{
         region: region,
         cnc_file: cnc_file,
         metadata: metadata,
         to_driver: to_driver,
         to_clients_address: addresses.to_clients,
         to_clients_length: metadata.to_clients_buffer_length,
         counters_metadata_address: addresses.counters_metadata,
         counters_values_address: addresses.counters_values,
         error_log_address: addresses.error_log
       }}
    end
  end

  @doc """
  Maps `cnc.dat` in `aeron_directory` once a live driver is serving it, retrying
  every #{@await_interval_ms} ms until `timeout_ms` has passed.

  The file is ready when it exists, its version has been written (the driver
  writes it last) and the driver's heartbeat is no older than `timeout_ms`.
  Returns `{:ok, cnc}` or `{:error, reason}` with the last reason the file was not
  ready: `:driver_inactive`, `:cnc_not_ready` or a mapping error such as
  `"open_failed"`.
  """
  @spec await_connect(String.t(), pos_integer()) :: {:ok, t()} | {:error, term()}
  def await_connect(aeron_directory, timeout_ms) do
    await_ready(aeron_directory, timeout_ms, System.monotonic_time(:millisecond) + timeout_ms)
  end

  @spec driver_alive?(t(), integer(), integer()) :: boolean()
  def driver_alive?(%__MODULE__{to_driver: to_driver}, now_ms, driver_timeout_ms) do
    MPSC.consumer_heartbeat_time(to_driver) >= now_ms - driver_timeout_ms
  end

  @doc """
  Returns the driver's last heartbeat as epoch milliseconds, or `-1` once the
  driver has shut down cleanly.
  """
  @spec driver_heartbeat_ms(t()) :: integer()
  def driver_heartbeat_ms(%__MODULE__{to_driver: to_driver}),
    do: MPSC.consumer_heartbeat_time(to_driver)

  defp await_ready(aeron_directory, timeout_ms, deadline) do
    aeron_directory
    |> ready_cnc(timeout_ms)
    |> retry_until_ready(aeron_directory, timeout_ms, deadline)
  end

  defp ready_cnc(aeron_directory, timeout_ms) do
    with {:ok, cnc} <- connect(aeron_directory),
         :ok <- version_written(cnc.metadata.cnc_version) do
      live_cnc(driver_alive?(cnc, System.system_time(:millisecond), timeout_ms), cnc)
    end
  end

  defp version_written(0), do: {:error, :cnc_not_ready}
  defp version_written(_version), do: :ok

  defp live_cnc(true, cnc), do: {:ok, cnc}
  defp live_cnc(false, _cnc), do: {:error, :driver_inactive}

  defp retry_until_ready({:ok, cnc}, _aeron_directory, _timeout_ms, _deadline), do: {:ok, cnc}

  defp retry_until_ready({:error, reason}, aeron_directory, timeout_ms, deadline) do
    retry_or_give_up(
      System.monotonic_time(:millisecond) >= deadline,
      reason,
      aeron_directory,
      timeout_ms,
      deadline
    )
  end

  defp retry_or_give_up(true, reason, _aeron_directory, _timeout_ms, _deadline),
    do: {:error, reason}

  defp retry_or_give_up(false, _reason, aeron_directory, timeout_ms, deadline) do
    Process.sleep(@await_interval_ms)
    await_ready(aeron_directory, timeout_ms, deadline)
  end

  @doc """
  Returns a receiver over the driver's to-clients broadcast buffer.

  The receiver reads the mapped buffer directly, so records and the counters they
  are validated against are always the transmitter's current values.
  """
  @spec to_clients(t()) :: Receiver.t()
  def to_clients(%__MODULE__{} = cnc) do
    Receiver.new(cnc.to_clients_address, cnc.to_clients_length)
  end

  @spec next_correlation_id(t()) :: integer()
  def next_correlation_id(%__MODULE__{to_driver: to_driver}) do
    MPSC.next_correlation_id(to_driver)
  end

  @spec write_command(t(), integer(), binary()) :: :ok | {:error, term()}
  def write_command(%__MODULE__{to_driver: to_driver}, message_type, command) do
    MPSC.write(to_driver, message_type, command)
  end

  defp verify_version(version) do
    version
    |> Bitwise.bsr(16)
    |> Bitwise.band(0xFFFF)
    |> verified_version(version)
  end

  defp verified_version(@cnc_version_major, _version), do: :ok
  defp verified_version(_major, version), do: {:error, {:cnc_version_mismatch, version}}

  defp resolve_addresses(cnc_file, metadata) do
    to_driver_offset = @cnc_version_and_meta_data_length
    to_clients_offset = to_driver_offset + metadata.to_driver_buffer_length
    counters_metadata_offset = to_clients_offset + metadata.to_clients_buffer_length
    counters_values_offset = counters_metadata_offset + metadata.counter_metadata_buffer_length
    error_log_offset = counters_values_offset + metadata.counter_values_buffer_length

    with {:ok, to_driver} <- NIF.get_buffer_address(cnc_file, to_driver_offset),
         {:ok, to_clients} <- NIF.get_buffer_address(cnc_file, to_clients_offset),
         {:ok, counters_metadata} <- NIF.get_buffer_address(cnc_file, counters_metadata_offset),
         {:ok, counters_values} <- NIF.get_buffer_address(cnc_file, counters_values_offset),
         {:ok, error_log} <- NIF.get_buffer_address(cnc_file, error_log_offset) do
      {:ok,
       %{
         to_driver: to_driver,
         to_clients: to_clients,
         counters_metadata: counters_metadata,
         counters_values: counters_values,
         error_log: error_log
       }}
    end
  end

  @counter_value_length 128

  @spec counter_value(t(), integer()) :: {:ok, integer()} | {:error, term()}
  def counter_value(%__MODULE__{counters_values_address: base}, counter_id)
      when is_integer(counter_id) and counter_id >= 0 do
    NIF.atomic_get_int64(base + counter_id * @counter_value_length)
  end

  @spec counter_address(t(), integer()) :: integer()
  def counter_address(%__MODULE__{counters_values_address: base}, counter_id)
      when is_integer(counter_id) and counter_id >= 0 do
    base + counter_id * @counter_value_length
  end
end
