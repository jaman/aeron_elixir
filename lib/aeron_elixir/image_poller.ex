defmodule AeronElixir.ImagePoller do
  @moduledoc """
  Image-side entry point onto the term-buffer data path.

  Resolves a single image by its driver correlation id from
  `AeronElixir.RuntimeHandles` and polls it directly from the calling process,
  reassembling fragments before invoking the handler. The image's owning client
  is carried on the record as `client_id`.
  """

  alias AeronElixir.ClientConductor
  alias AeronElixir.LogBuffer.Subscriber
  alias AeronElixir.RuntimeHandles

  @doc """
  Polls the image with `correlation_id` with `handler` and returns the number of messages delivered.
  """
  @spec poll(map(), pos_integer(), (binary(), AeronElixir.Header.t() -> term())) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def poll(%{client_id: client_id, correlation_id: correlation_id}, fragment_limit, handler)
      when is_integer(fragment_limit) and fragment_limit > 0 and is_function(handler, 2) do
    with {:ok, image} <- RuntimeHandles.fetch_image(client_id, correlation_id) do
      {:ok, Subscriber.poll(image, fragment_limit, handler)}
    end
  end

  @doc """
  Polls the image honouring the handler's `:continue | :commit | :break | :abort` result.
  """
  @spec controlled_poll(map(), pos_integer(), (binary(), AeronElixir.Header.t() -> term())) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def controlled_poll(image, fragment_limit, handler) do
    poll(image, fragment_limit, handler)
  end

  @doc """
  Drops the image from the owning client; `reason` is informational.
  """
  @spec reject(map(), String.t()) :: :ok | {:error, :closed}
  def reject(%{client_id: client_id, correlation_id: correlation_id}, _reason) do
    ClientConductor.remove_image(client_id, correlation_id)
  end
end
