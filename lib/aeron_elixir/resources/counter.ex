defmodule AeronElixir.Resources.Counter do
  @moduledoc """
  Ash resource for Aeron counters.

  Counters are distributed values managed by the media driver.
  """

  use Ash.Resource,
    domain: AeronElixir.Domain,
    data_layer: Ash.DataLayer.Ets

  attributes do
    uuid_primary_key(:id)

    attribute :counter_id, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :registration_id, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :value_address, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :type_id, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :key, :binary do
      allow_nil?(false)
      public?(true)
    end

    attribute :label, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :value, :integer do
      default(0)
      public?(true)
    end

    attribute :owner_id, :integer do
      default(0)
      public?(true)
    end

    attribute :reference_id, :integer do
      default(0)
      public?(true)
    end

    attribute :state, :atom do
      default(:allocated)
      constraints(one_of: [:unused, :allocated, :reclaimed])
      public?(true)
    end

    attribute :is_closed, :boolean do
      default(false)
      public?(true)
    end

    attribute :is_static, :boolean do
      default(false)
      public?(true)
    end

    create_timestamp(:inserted_at)
    update_timestamp(:updated_at)
  end

  relationships do
    belongs_to :client, AeronElixir.Resources.Client do
      allow_nil?(false)
    end
  end

  actions do
    defaults([:read, :update, :destroy])

    create :add_counter do
      primary?(true)
      argument(:client_id, :uuid)
      argument(:type_id, :integer)
      argument(:key, :binary)
      argument(:label, :string)
      argument(:is_static, :boolean, default: false)
      argument(:static_registration_id, :integer, default: nil)

      change(set_attribute(:client_id, arg(:client_id)))
      change(set_attribute(:type_id, arg(:type_id)))
      change(set_attribute(:key, arg(:key)))
      change(set_attribute(:label, arg(:label)))
      change(set_attribute(:is_static, arg(:is_static)))
      change(set_attribute(:state, :allocated))
      change(AeronElixir.Changes.InitCounter)
    end

    update :increment do
      require_atomic?(false)
      argument(:delta, :integer, default: 1)

      change(AeronElixir.Changes.IncrementCounter)
    end

    update :set do
      require_atomic?(false)
      argument(:value, :integer)

      change(set_attribute(:value, arg(:value)))
      change(AeronElixir.Changes.SetCounter)
    end

    update :close do
      accept([])
      require_atomic?(false)
      change(set_attribute(:is_closed, true))
      change(AeronElixir.Changes.CloseCounter)
    end
  end

  code_interface do
    define(:add_counter, args: [:client_id, :type_id, :key, :label])
    define(:increment, args: [{:optional, :delta}])
    define(:set, args: [:value])
    define(:close)
    define(:read)
  end
end
