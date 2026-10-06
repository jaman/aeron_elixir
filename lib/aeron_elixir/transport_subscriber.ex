defmodule AeronElixir.TransportSubscriber do
  @moduledoc """
  Subscription-side entry point onto the term-buffer data path.

  Finds the subscription's images through `AeronElixir.ImageCache`, which reuses
  them until they change, and polls each one directly from the calling process;
  the conductor is not involved.
  Fragmented messages are reassembled before the handler is invoked. Returns the
  number of messages delivered across all images. Only one process may poll a
  given subscription at a time.
  """

  alias AeronElixir.ClientConductor
  alias AeronElixir.ImageCache
  alias AeronElixir.LogBuffer.Subscriber

  @doc """
  Polls every image of the subscription with `handler` and returns the number of messages delivered.
  """
  @spec poll(map(), pos_integer(), (binary(), AeronElixir.Header.t() -> term())) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def poll(subscription, fragment_limit, handler)
      when is_integer(fragment_limit) and fragment_limit > 0 and is_function(handler, 2) do
    with {:ok, images} <- ImageCache.images(subscription) do
      {:ok, Enum.reduce(images, 0, &(Subscriber.poll(&1, fragment_limit, handler) + &2))}
    end
  end

  @doc """
  Polls every image honouring the handler's `:continue | :commit | :break | :abort` result.
  """
  @spec controlled_poll(map(), pos_integer(), (binary(), AeronElixir.Header.t() -> term())) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def controlled_poll(subscription, fragment_limit, handler) do
    poll(subscription, fragment_limit, handler)
  end

  @doc """
  Removes the subscription from the driver.
  """
  @spec close(map()) :: :ok | {:error, term()}
  def close(%{client_id: client_id, registration_id: registration_id}) do
    ClientConductor.remove_subscription(client_id, registration_id)
  end
end
