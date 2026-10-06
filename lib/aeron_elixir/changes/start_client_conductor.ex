defmodule AeronElixir.Changes.StartClientConductor do
  @moduledoc """
  Ash change to start the client conductor process.
  """

  use Ash.Resource.Change

  alias AeronElixir.ClientConductor

  @impl true
  def change(changeset, _opts, _context) do
    Ash.Changeset.after_action(changeset, &start_conductor/2)
  end

  defp start_conductor(_changeset, record) do
    record
    |> start_child()
    |> conductor_started(record)
  end

  defp start_child(client) do
    child_spec = %{
      id: {ClientConductor, client.id},
      start: {ClientConductor, :start_link, [client]},
      restart: :transient
    }

    DynamicSupervisor.start_child(AeronElixir.ClientSupervisor, child_spec)
  end

  defp conductor_started({:ok, _pid}, record), do: {:ok, record}
  defp conductor_started({:error, {:already_started, _pid}}, record), do: {:ok, record}
  defp conductor_started({:error, reason}, _record), do: {:error, reason}
end
