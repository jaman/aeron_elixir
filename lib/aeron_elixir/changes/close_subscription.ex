defmodule AeronElixir.Changes.CloseSubscription do
  @moduledoc false
  use Ash.Resource.Change

  @impl true
  def change(changeset, _opts, _context) do
    Ash.Changeset.after_action(changeset, fn _changeset, record ->
      AeronElixir.TransportSubscriber.close(record)
      {:ok, record}
    end)
  end
end
