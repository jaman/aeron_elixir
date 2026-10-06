defmodule AeronElixir.TransportPublisher do
  @moduledoc """
  Publication-side entry point onto the term-buffer data path.

  The publish functions take a publication record and append through the
  `AeronElixir.LogBuffer.Publisher` handle it carries, directly into the mapped
  log buffer from the calling process; neither the conductor nor a table lookup
  is involved. `fetch_handle/2` resolves the handle from `AeronElixir.RuntimeHandles`
  instead, and reports `{:error, :closed}` once the publication is closed.

  `publish/3` retries a back-pressured, not-yet-connected or rotating publication
  until the client's publish deadline, then returns the last status
  (`{:error, :back_pressured}` or `{:error, :not_connected}`). `try_publish/3`
  makes a single attempt and returns the status as is. Exceeding the log's
  maximum position and a closed publication are never retried; once the
  publication is closed, by `close/1` or because its client lost the driver,
  every publish function returns `{:error, :closed}`.
  """

  alias AeronElixir.ClientConductor
  alias AeronElixir.LogBuffer.Publisher
  alias AeronElixir.RuntimeHandles

  @retry_interval_ms 1

  @doc """
  Publishes `message` with `reserved_value`, retrying back-pressure until the client's publish deadline.
  """
  @spec publish(map(), iodata(), integer()) :: {:ok, integer()} | {:error, term()}
  def publish(%{handle: %Publisher{} = handle, client_id: client_id}, message, reserved_value)
      when (is_binary(message) or is_list(message)) and is_integer(reserved_value),
      do: publish_until_deadline(handle, message, reserved_value, client_id, nil)

  @doc """
  Publishes `message` once and returns the offer status without retrying.
  """
  @spec try_publish(map(), iodata(), integer()) :: {:ok, integer()} | {:error, term()}
  def try_publish(%{handle: %Publisher{} = handle}, message, reserved_value)
      when (is_binary(message) or is_list(message)) and is_integer(reserved_value),
      do: Publisher.publish(handle, message, reserved_value)

  @doc """
  Resolves the runtime handle for a publication, reporting `:closed` when the conductor no longer holds one.
  """
  @spec fetch_handle(term(), integer()) ::
          {:ok, Publisher.t()} | {:error, :closed}
  def fetch_handle(client_id, registration_id) do
    closed_when_unknown(RuntimeHandles.fetch_publication(client_id, registration_id))
  end

  defp closed_when_unknown({:error, :unknown_publication}), do: {:error, :closed}
  defp closed_when_unknown(result), do: result

  @doc """
  Publishes `count` copies of `message` in one native call and returns how many were appended.
  """
  @spec publish_n(map(), binary(), integer(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def publish_n(%{handle: %Publisher{} = handle}, message, reserved_value, count)
      when is_binary(message) and is_integer(reserved_value) and is_integer(count) and count >= 0,
      do: handle |> Publisher.publish_n(message, reserved_value, count) |> appended_result()

  @doc """
  Publishes each element of `payloads` in one native call and returns how many were appended.
  """
  @spec publish_list(map(), [iodata()], integer()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def publish_list(%{handle: %Publisher{} = handle}, payloads, reserved_value)
      when is_list(payloads) and is_integer(reserved_value),
      do: handle |> Publisher.publish_list(payloads, reserved_value) |> appended_result()

  @doc """
  Removes the publication from the driver.
  """
  @spec close(map()) :: :ok | {:error, term()}
  def close(%{client_id: client_id, registration_id: registration_id}) do
    ClientConductor.remove_publication(client_id, registration_id)
  end

  defp appended_result({:error, :closed} = closed), do: closed
  defp appended_result(appended), do: {:ok, appended}

  defp publish_until_deadline(handle, message, reserved_value, client_id, deadline) do
    handle
    |> Publisher.publish(message, reserved_value)
    |> retry_unless_final(handle, message, reserved_value, client_id, deadline)
  end

  defp retry_unless_final({:ok, position}, _handle, _message, _reserved, _client_id, _deadline),
    do: {:ok, position}

  defp retry_unless_final(
         {:error, final} = error,
         _handle,
         _message,
         _reserved,
         _client_id,
         _deadline
       )
       when final in [:max_position_exceeded, :closed],
       do: error

  defp retry_unless_final(
         {:error, retryable},
         handle,
         message,
         reserved_value,
         client_id,
         deadline
       ),
       do: retry_after_interval(handle, message, reserved_value, client_id, deadline, retryable)

  defp retry_after_interval(handle, message, reserved_value, client_id, nil, status) do
    with {:ok, deadline} <- RuntimeHandles.publish_deadline(client_id) do
      retry_after_interval(handle, message, reserved_value, client_id, deadline, status)
    end
  end

  defp retry_after_interval(handle, message, reserved_value, client_id, deadline, status) do
    retry_or_give_up(
      System.monotonic_time(:millisecond) >= deadline,
      handle,
      message,
      reserved_value,
      client_id,
      deadline,
      status
    )
  end

  defp retry_or_give_up(true, _handle, _message, _reserved, _client_id, _deadline, status),
    do: {:error, status}

  defp retry_or_give_up(false, handle, message, reserved_value, client_id, deadline, _status) do
    Process.sleep(@retry_interval_ms)
    publish_until_deadline(handle, message, reserved_value, client_id, deadline)
  end
end
