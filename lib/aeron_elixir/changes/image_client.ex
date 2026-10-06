defmodule AeronElixir.Changes.ImageClient do
  @moduledoc """
  Resolves the owning client id for an image.

  An image belongs to a subscription, which belongs to a client. The conductor is
  registered per client, so polling or rejecting an image needs the client id of
  its subscription. This loads the relationship through Ash rather than reaching
  into the data layer directly.
  """

  alias AeronElixir.Resources.Image

  @spec client_id(Image.t()) :: {:ok, term()} | {:error, term()}
  def client_id(%{subscription_id: subscription_id} = image) do
    image
    |> Ash.load(subscription: [:client_id])
    |> loaded_client_id(subscription_id)
  end

  defp loaded_client_id({:ok, %{subscription: %{client_id: client_id}}}, _subscription_id),
    do: {:ok, client_id}

  defp loaded_client_id({:ok, _image}, subscription_id),
    do: {:error, {:subscription_not_loaded, subscription_id}}

  defp loaded_client_id({:error, reason}, _subscription_id), do: {:error, reason}
end
