defmodule AeronElixir.Application do
  @moduledoc false

  use Application

  alias AeronElixir.DriverConfig
  alias AeronElixir.MediaDriver

  @impl true
  def start(_type, _args) do
    children =
      [{Registry, keys: :unique, name: AeronElixir.Registry}] ++
        driver_children(DriverConfig.resolve()) ++
        [{DynamicSupervisor, strategy: :one_for_one, name: AeronElixir.ClientSupervisor}]

    Supervisor.start_link(children, strategy: :rest_for_one, name: AeronElixir.Supervisor)
  end

  defp driver_children(%DriverConfig{mode: :embedded} = config) do
    [
      {MediaDriver,
       name: AeronElixir.EmbeddedDriver,
       aeron_dir: config.aeron_dir,
       binary: config.binary,
       remove_directory: config.private?,
       sweep_stale: config.private?}
    ]
  end

  defp driver_children(%DriverConfig{mode: :external}), do: []
end
