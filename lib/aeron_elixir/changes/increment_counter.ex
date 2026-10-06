defmodule AeronElixir.Changes.IncrementCounter do
  @moduledoc """
  Atomically applies a delta to the counter's shared-memory value and reflects
  the resulting value back onto the record.
  """

  use Ash.Resource.Change

  alias AeronElixir.ClientConductor

  @impl true
  def change(changeset, _opts, _context) do
    delta = Ash.Changeset.get_argument(changeset, :delta)

    Ash.Changeset.after_action(changeset, fn _changeset, record ->
      record.client_id
      |> ClientConductor.increment_counter(record.value_address, delta)
      |> incremented_value(record)
    end)
  end

  defp incremented_value({:ok, value}, record), do: {:ok, %{record | value: value}}
  defp incremented_value({:error, reason}, _record), do: {:error, reason}
end
