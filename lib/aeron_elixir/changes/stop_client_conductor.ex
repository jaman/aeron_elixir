defmodule AeronElixir.Changes.StopClientConductor do
  @moduledoc """
  Ash change to stop the client conductor process.
  """

  use Ash.Resource.Change

  @impl true
  def change(changeset, _opts, _context) do
    Ash.Changeset.after_action(changeset, fn _changeset, record ->
      stop_conductor(record.id)
      {:ok, record}
    end)
  end

  defp stop_conductor(client_id) do
    AeronElixir.Registry
    |> Registry.lookup({:client_conductor, client_id})
    |> terminate_conductor()
  end

  defp terminate_conductor([{pid, _value}]),
    do: DynamicSupervisor.terminate_child(AeronElixir.ClientSupervisor, pid)

  defp terminate_conductor([]), do: :ok
end
