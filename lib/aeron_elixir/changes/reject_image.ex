defmodule AeronElixir.Changes.RejectImage do
  @moduledoc false
  use Ash.Resource.Change

  alias AeronElixir.Changes.ImageClient
  alias AeronElixir.ImagePoller

  @impl true
  def change(changeset, _opts, _context) do
    rejection_reason = Ash.Changeset.get_argument(changeset, :reason)

    Ash.Changeset.after_action(changeset, fn _changeset, record ->
      record
      |> ImageClient.client_id()
      |> reject_for_client(record, rejection_reason)
    end)
  end

  defp reject_for_client({:ok, client_id}, record, rejection_reason) do
    ImagePoller.reject(
      %{client_id: client_id, correlation_id: record.correlation_id},
      rejection_reason
    )

    {:ok, record}
  end

  defp reject_for_client({:error, reason}, _record, _rejection_reason), do: {:error, reason}
end
