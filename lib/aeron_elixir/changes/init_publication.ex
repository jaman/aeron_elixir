defmodule AeronElixir.Changes.InitPublication do
  @moduledoc """
  Performs the driver handshake for a publication and populates driver-assigned
  attributes before the create action validates required fields.
  """

  use Ash.Resource.Change

  alias AeronElixir.ClientConductor

  @driver_fields [
    :handle,
    :registration_id,
    :session_id,
    :initial_term_id,
    :term_buffer_length,
    :max_message_length,
    :max_payload_length,
    :position_bits_to_shift,
    :max_possible_position,
    :publication_limit_counter_id,
    :channel_status_indicator_id
  ]

  @impl true
  def change(changeset, _opts, _context) do
    Ash.Changeset.before_action(changeset, &handshake/1)
  end

  defp handshake(changeset) do
    client_id = Ash.Changeset.get_argument(changeset, :client_id)
    channel = Ash.Changeset.get_attribute(changeset, :channel_uri)
    stream_id = Ash.Changeset.get_attribute(changeset, :stream_id)
    exclusive? = Ash.Changeset.get_argument(changeset, :is_exclusive) == true

    client_id
    |> ClientConductor.add_publication(channel, stream_id, exclusive?)
    |> apply_driver_fields(changeset)
  end

  defp apply_driver_fields({:ok, fields}, changeset) do
    Enum.reduce(@driver_fields, changeset, fn field, acc ->
      Ash.Changeset.force_change_attribute(acc, field, Map.fetch!(fields, field))
    end)
  end

  defp apply_driver_fields({:error, reason}, changeset),
    do: Ash.Changeset.add_error(changeset, field: :channel_uri, message: inspect(reason))
end
