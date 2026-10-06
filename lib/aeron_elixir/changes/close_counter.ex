defmodule AeronElixir.Changes.CloseCounter do
  @moduledoc """
  Removes the counter from the driver via the owning client conductor.
  """

  use Ash.Resource.Change

  alias AeronElixir.ClientConductor

  @impl true
  def change(changeset, _opts, _context) do
    Ash.Changeset.after_action(changeset, &remove_counter/2)
  end

  defp remove_counter(_changeset, record) do
    record.client_id
    |> ClientConductor.remove_counter(record.registration_id)
    |> counter_removal(record)
  end

  defp counter_removal(:ok, record), do: {:ok, record}
  defp counter_removal({:error, :closed}, record), do: {:ok, record}
  defp counter_removal({:error, reason}, _record), do: {:error, reason}
end
