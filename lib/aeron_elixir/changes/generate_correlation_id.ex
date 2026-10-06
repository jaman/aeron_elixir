defmodule AeronElixir.Changes.GenerateCorrelationId do
  @moduledoc """
  Ash change to generate unique correlation IDs.
  """

  use Ash.Resource.Change

  @impl true
  def atomic(changeset, _opts, _context) do
    {:atomic, %{Ash.Changeset.get_attribute(changeset, :id) => gen_correlation_id()}}
  end

  @doc """
  Generates a unique, positive, monotonic correlation id.

  Used as the zero-arity value generator for `set_attribute/2` on registration
  id attributes.
  """
  @spec generate() :: pos_integer()
  def generate, do: gen_correlation_id()

  defp gen_correlation_id do
    System.unique_integer([:positive, :monotonic])
  end
end
