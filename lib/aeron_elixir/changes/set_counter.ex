defmodule AeronElixir.Changes.SetCounter do
  @moduledoc """
  Writes an absolute value into the counter's shared-memory slot via the owning
  client conductor.
  """

  use Ash.Resource.Change

  alias AeronElixir.CounterManager

  @impl true
  def change(changeset, _opts, _context) do
    value = Ash.Changeset.get_argument(changeset, :value)

    Ash.Changeset.after_action(changeset, fn _changeset, record ->
      record.value_address
      |> CounterManager.set(value)
      |> counter_written(record)
    end)
  end

  defp counter_written({:ok, _value}, record), do: {:ok, record}
  defp counter_written({:error, reason}, _record), do: {:error, reason}
end
