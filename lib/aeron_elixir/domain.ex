defmodule AeronElixir.Domain do
  @moduledoc """
  Ash domain wiring the Aeron client resources.

  Registering the resources here is what allows each resource's
  `code_interface` (e.g. `Client.connect/1`) to be generated and what lets
  Ash run their actions.
  """

  use Ash.Domain, validate_config_inclusion?: false

  resources do
    resource(AeronElixir.Resources.Client)
    resource(AeronElixir.Resources.Publication)
    resource(AeronElixir.Resources.Subscription)
    resource(AeronElixir.Resources.Image)
    resource(AeronElixir.Resources.Counter)
  end
end
