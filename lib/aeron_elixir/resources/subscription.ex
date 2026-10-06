defmodule AeronElixir.Resources.Subscription do
  @moduledoc """
  Ash resource for Aeron subscriptions.

  Subscriptions receive messages from publications via images.
  """

  use Ash.Resource,
    domain: AeronElixir.Domain,
    data_layer: Ash.DataLayer.Ets

  alias AeronElixir.Changes.GenerateCorrelationId

  attributes do
    uuid_primary_key(:id)

    attribute :registration_id, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :channel_uri, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :stream_id, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :channel_status, :atom do
      allow_nil?(false)
      default(:active)
      constraints(one_of: [:active, :errored])
      public?(true)
    end

    attribute :channel_status_indicator_id, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :image_version, :term do
      allow_nil?(false)
      public?(false)

      description(
        "The client's image version counter; `AeronElixir.ImageCache` reads it to reuse this subscription's images until they change."
      )
    end

    attribute :is_connected, :boolean do
      default(false)
      public?(true)
    end

    attribute :is_closed, :boolean do
      default(false)
      public?(true)
    end

    attribute :image_count, :integer do
      default(0)
      public?(true)
    end

    attribute :round_robin_index, :integer do
      default(0)
      public?(true)
    end

    attribute :last_poll_count, :integer do
      default(0)
      public?(true)
    end

    create_timestamp(:inserted_at)
    update_timestamp(:updated_at)
  end

  relationships do
    belongs_to :client, AeronElixir.Resources.Client do
      allow_nil?(false)
    end

    has_many(:images, AeronElixir.Resources.Image)
  end

  actions do
    defaults([:read, :update, :destroy])

    create :add_subscription do
      primary?(true)
      argument(:client_id, :uuid)
      argument(:channel_uri, :string)
      argument(:stream_id, :integer)
      argument(:available_image_handler, :term)
      argument(:unavailable_image_handler, :term)

      change(set_attribute(:registration_id, &GenerateCorrelationId.generate/0))

      change(set_attribute(:channel_uri, arg(:channel_uri)))
      change(set_attribute(:stream_id, arg(:stream_id)))

      change(set_attribute(:channel_status, :active))
      change(set_attribute(:client_id, arg(:client_id)))
      change(AeronElixir.Changes.InitSubscription)
    end

    update :poll do
      require_atomic?(false)
      argument(:fragment_limit, :integer, default: 10)
      argument(:fragment_handler, :term)

      change(AeronElixir.Changes.PollSubscription)
    end

    update :controlled_poll do
      require_atomic?(false)
      argument(:fragment_limit, :integer, default: 10)
      argument(:fragment_handler, :term)

      change(AeronElixir.Changes.ControlledPollSubscription)
    end

    update :close do
      accept([])
      require_atomic?(false)
      change(set_attribute(:is_closed, true))
      change(set_attribute(:is_connected, false))
      change(AeronElixir.Changes.CloseSubscription)
    end
  end

  code_interface do
    define(:add_subscription,
      args: [
        :client_id,
        :channel_uri,
        :stream_id,
        :available_image_handler,
        :unavailable_image_handler
      ]
    )

    define(:poll, args: [{:optional, :fragment_limit}, :fragment_handler])
    define(:controlled_poll, args: [{:optional, :fragment_limit}, :fragment_handler])
    define(:close, args: [])
    define(:read)
  end
end
