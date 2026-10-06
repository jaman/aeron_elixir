defmodule AeronElixir.Changes.ControlledPollSubscription do
  @moduledoc false
  use Ash.Resource.Change

  alias AeronElixir.TransportSubscriber

  @impl true
  def change(changeset, _opts, _context) do
    fragment_limit = Ash.Changeset.get_argument(changeset, :fragment_limit)
    fragment_handler = Ash.Changeset.get_argument(changeset, :fragment_handler)

    Ash.Changeset.after_action(changeset, fn _changeset, record ->
      record
      |> TransportSubscriber.controlled_poll(fragment_limit, fragment_handler)
      |> poll_result(record)
    end)
  end

  defp poll_result({:ok, count}, record), do: {:ok, %{record | last_poll_count: count}}
  defp poll_result({:error, reason}, _record), do: {:error, reason}
end
