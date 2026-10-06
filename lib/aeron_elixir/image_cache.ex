defmodule AeronElixir.ImageCache do
  @moduledoc """
  The images of a subscription as the calling process last read them, reused
  until they change.

  `images/1` takes a `%Subscription{}` record and reads its client's image
  version, a counter that rises whenever an image arrives or leaves on any of the
  client's subscriptions and when the client stops. While the version matches
  the one cached in the calling process's dictionary, the cached images are
  returned without touching the client's handle table; otherwise they are read
  again with `AeronElixir.RuntimeHandles.images/2` and cached under the new
  version.

  Each process that polls a subscription keeps its own small entry, holding the
  image handles. A cached handle stays safe to use after its image closes: the
  poll functions read nothing from a closed image, and the next version change
  replaces the entry.
  """

  alias AeronElixir.LogBuffer.Subscriber
  alias AeronElixir.Resources.Subscription
  alias AeronElixir.RuntimeHandles

  @doc """
  Returns `{:ok, images}` for `subscription`, or `{:error, reason}` as
  `AeronElixir.RuntimeHandles.images/2` does once the client has stopped.
  """
  @spec images(Subscription.t()) :: {:ok, [Subscriber.t()]} | {:error, term()}
  def images(%{
        client_id: client_id,
        registration_id: registration_id,
        image_version: image_version
      }) do
    version = :atomics.get(image_version, 1)
    key = {__MODULE__, client_id, registration_id}
    cached(Process.get(key), version, key, client_id, registration_id)
  end

  defp cached({version, images}, version, _key, _client_id, _registration_id), do: {:ok, images}

  defp cached(_stale, version, key, client_id, registration_id),
    do: client_id |> RuntimeHandles.images(registration_id) |> remember(key, version)

  defp remember({:ok, images} = found, key, version) do
    Process.put(key, {version, images})
    found
  end

  defp remember(error, key, _version) do
    Process.delete(key)
    error
  end
end
