defmodule AeronElixir.Changes.InitCounter do
  @moduledoc """
  Performs the driver handshake for a counter and populates the driver-assigned
  registration id, counter id and value address before the create action
  validates required fields.
  """

  use Ash.Resource.Change

  alias AeronElixir.ClientConductor

  @impl true
  def change(changeset, _opts, _context) do
    Ash.Changeset.before_action(changeset, &handshake/1)
  end

  defp handshake(changeset) do
    client_id = Ash.Changeset.get_argument(changeset, :client_id)
    type_id = Ash.Changeset.get_attribute(changeset, :type_id)
    key = Ash.Changeset.get_attribute(changeset, :key)
    label = Ash.Changeset.get_attribute(changeset, :label)

    client_id
    |> ClientConductor.add_counter(type_id, key, label)
    |> apply_counter_fields(changeset)
  end

  defp apply_counter_fields({:ok, fields}, changeset) do
    changeset
    |> Ash.Changeset.force_change_attribute(:registration_id, fields.registration_id)
    |> Ash.Changeset.force_change_attribute(:counter_id, fields.counter_id)
    |> Ash.Changeset.force_change_attribute(:value_address, fields.value_address)
  end

  defp apply_counter_fields({:error, reason}, changeset),
    do: Ash.Changeset.add_error(changeset, field: :label, message: inspect(reason))
end
