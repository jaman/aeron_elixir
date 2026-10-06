defmodule AeronElixir.Changes.PublishMessage do
  @moduledoc false
  use Ash.Resource.Change

  alias AeronElixir.TransportPublisher

  @impl true
  def change(changeset, _opts, _context) do
    message = Ash.Changeset.get_argument(changeset, :message)
    reserved_value = Ash.Changeset.get_argument(changeset, :reserved_value)

    Ash.Changeset.after_action(changeset, fn _changeset, record ->
      record
      |> TransportPublisher.publish(message, reserved_value)
      |> published_position(record)
    end)
  end

  defp published_position({:ok, position}, record),
    do: {:ok, %{record | current_position: position}}

  defp published_position({:error, reason}, _record), do: {:error, reason}
end
