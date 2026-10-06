defmodule AeronElixir.Changes.PollImage do
  @moduledoc false
  use Ash.Resource.Change

  alias AeronElixir.Changes.ImageClient

  @impl true
  def change(changeset, _opts, _context) do
    fragment_limit = Ash.Changeset.get_argument(changeset, :fragment_limit)
    fragment_handler = Ash.Changeset.get_argument(changeset, :fragment_handler)

    Ash.Changeset.after_action(changeset, fn _changeset, record ->
      with {:ok, client_id} <- ImageClient.client_id(record),
           {:ok, count} <-
             AeronElixir.ImagePoller.poll(
               %{client_id: client_id, correlation_id: record.correlation_id},
               fragment_limit,
               fragment_handler
             ) do
        {:ok, %{record | last_poll_count: count}}
      end
    end)
  end
end
