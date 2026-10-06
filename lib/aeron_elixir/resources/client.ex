defmodule AeronElixir.Resources.Client do
  @moduledoc """
  Ash resource for Aeron client configuration and lifecycle.

  Manages connection to Aeron media driver with async resource management.
  """

  use Ash.Resource,
    domain: AeronElixir.Domain,
    data_layer: Ash.DataLayer.Ets

  attributes do
    uuid_primary_key(:id)

    attribute :aeron_directory, :string do
      allow_nil?(false)
      default(&AeronElixir.DriverConfig.aeron_dir/0)
      public?(true)
    end

    attribute :driver_timeout_ms, :integer do
      allow_nil?(false)
      default(5000)
      constraints(min: 100)
      public?(true)
    end

    attribute :client_linger_timeout_ns, :integer do
      allow_nil?(false)
      default(5_000_000_000)
      public?(true)
    end

    attribute :keepalive_interval_ns, :integer do
      allow_nil?(false)
      default(500_000_000)
      public?(true)
    end

    attribute :media_driver_heartbeat_interval_ns, :integer do
      allow_nil?(false)
      default(1_000_000_000)
      public?(true)
    end

    attribute :resource_linger_timeout_ns, :integer do
      allow_nil?(false)
      default(5_000_000_000)
      public?(true)
    end

    attribute :inter_service_timeout_ns, :integer do
      allow_nil?(false)
      default(5_000_000_000)
      public?(true)
    end

    attribute :pre_touch_mapped_memory, :boolean do
      default(false)
      public?(true)
    end

    attribute :client_name, :string do
      default(nil)
      public?(true)
    end

    attribute :agent_on_start_function, :atom do
      default(nil)
      public?(true)
    end

    attribute :use_conductor_agent_invoker, :boolean do
      default(false)
      public?(true)
    end

    attribute :idle_strategy, :atom do
      default(:backoff)
      public?(true)
    end

    attribute :idle_strategy_init_args, :string do
      default(nil)
      public?(true)
    end

    attribute :status, :atom do
      allow_nil?(false)
      default(:connecting)
      constraints(one_of: [:connecting, :connected, :closed, :error])
      public?(true)
    end

    create_timestamp(:inserted_at)
    update_timestamp(:updated_at)
  end

  relationships do
    has_many(:publications, AeronElixir.Resources.Publication)
    has_many(:subscriptions, AeronElixir.Resources.Subscription)
    has_many(:counters, AeronElixir.Resources.Counter)
  end

  actions do
    defaults([:read, :update, :destroy])

    create :connect do
      primary?(true)
      argument(:aeron_directory, :string)
      argument(:driver_timeout_ms, :integer)
      argument(:client_linger_timeout_ns, :integer)
      argument(:keepalive_interval_ns, :integer)
      argument(:media_driver_heartbeat_interval_ns, :integer)
      argument(:resource_linger_timeout_ns, :integer)
      argument(:inter_service_timeout_ns, :integer)
      argument(:pre_touch_mapped_memory, :boolean)
      argument(:client_name, :string)
      argument(:agent_on_start_function, :atom)
      argument(:use_conductor_agent_invoker, :boolean)
      argument(:idle_strategy, :atom)
      argument(:idle_strategy_init_args, :string)

      change(set_attribute(:status, :connecting))
      change(AeronElixir.Changes.MapConnectArguments)
      change(AeronElixir.Changes.StartClientConductor)
    end

    update :close do
      accept([])
      require_atomic?(false)
      change(set_attribute(:status, :closed))
      change(AeronElixir.Changes.StopClientConductor)
    end
  end

  code_interface do
    define(:connect)
    define(:close)
    define(:read)
  end
end
