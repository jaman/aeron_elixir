defmodule AeronElixir.Changes.MapConnectArguments do
  @moduledoc """
  Copies provided connect arguments onto their matching attributes, leaving
  attribute defaults intact for arguments the caller did not supply.
  """

  use Ash.Resource.Change

  @mapped_arguments [
    :aeron_directory,
    :driver_timeout_ms,
    :client_linger_timeout_ns,
    :keepalive_interval_ns,
    :media_driver_heartbeat_interval_ns,
    :resource_linger_timeout_ns,
    :inter_service_timeout_ns,
    :pre_touch_mapped_memory,
    :client_name,
    :agent_on_start_function,
    :use_conductor_agent_invoker,
    :idle_strategy,
    :idle_strategy_init_args
  ]

  @impl true
  def change(changeset, _opts, _context) do
    Enum.reduce(@mapped_arguments, changeset, &apply_argument/2)
  end

  defp apply_argument(argument, changeset) do
    changeset
    |> Ash.Changeset.fetch_argument(argument)
    |> apply_argument_value(argument, changeset)
  end

  defp apply_argument_value({:ok, nil}, _argument, changeset), do: changeset

  defp apply_argument_value({:ok, value}, argument, changeset),
    do: Ash.Changeset.force_change_attribute(changeset, argument, value)

  defp apply_argument_value(:error, _argument, changeset), do: changeset
end
