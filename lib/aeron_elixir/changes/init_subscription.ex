defmodule AeronElixir.Changes.InitSubscription do
  @moduledoc """
  Performs the driver handshake for a subscription and populates the
  driver-assigned channel status indicator before the create action validates.
  """

  use Ash.Resource.Change

  alias AeronElixir.ClientConductor

  @impl true
  def change(changeset, _opts, _context) do
    Ash.Changeset.before_action(changeset, &handshake/1)
  end

  defp handshake(changeset) do
    client_id = Ash.Changeset.get_argument(changeset, :client_id)
    channel = Ash.Changeset.get_attribute(changeset, :channel_uri)
    stream_id = Ash.Changeset.get_attribute(changeset, :stream_id)

    handlers = %{
      available: Ash.Changeset.get_argument(changeset, :available_image_handler),
      unavailable: Ash.Changeset.get_argument(changeset, :unavailable_image_handler)
    }

    client_id
    |> ClientConductor.add_subscription(channel, stream_id, handlers)
    |> apply_subscription_fields(changeset)
  end

  defp apply_subscription_fields({:ok, fields}, changeset) do
    changeset
    |> Ash.Changeset.force_change_attribute(:registration_id, fields.registration_id)
    |> Ash.Changeset.force_change_attribute(
      :channel_status_indicator_id,
      fields.channel_status_indicator_id
    )
    |> Ash.Changeset.force_change_attribute(:image_version, fields.image_version)
  end

  defp apply_subscription_fields({:error, reason}, changeset),
    do: Ash.Changeset.add_error(changeset, field: :channel_uri, message: inspect(reason))
end
