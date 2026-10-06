defmodule AeronElixir.Resources.Publication do
  @moduledoc """
  Ash resource for Aeron publications.

  Publications are used to send messages to subscribers.
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

    attribute :original_registration_id, :integer do
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

    attribute :session_id, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :initial_term_id, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :max_possible_position, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :position_bits_to_shift, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :term_buffer_length, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :max_message_length, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :max_payload_length, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :publication_limit_counter_id, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :channel_status_indicator_id, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :handle, :term do
      allow_nil?(false)
      public?(false)

      description(
        "The `AeronElixir.LogBuffer.Publisher` handle the publish functions append through; it returns `{:error, :closed}` once the publication is closed."
      )
    end

    attribute :channel_status, :atom do
      allow_nil?(false)
      default(:active)
      constraints(one_of: [:active, :errored])
      public?(true)
    end

    attribute :is_connected, :boolean do
      default(false)
      public?(true)
    end

    attribute :is_closed, :boolean do
      default(false)
      public?(true)
    end

    attribute :is_exclusive, :boolean do
      default(false)
      public?(true)
    end

    attribute :current_position, :integer do
      default(0)
      public?(true)
    end

    attribute :position_limit, :integer do
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
  end

  actions do
    defaults([:read, :update, :destroy])

    create :add_publication do
      primary?(true)
      argument(:client_id, :uuid)
      argument(:channel_uri, :string)
      argument(:stream_id, :integer)
      argument(:is_exclusive, :boolean, default: false)

      change(set_attribute(:registration_id, &GenerateCorrelationId.generate/0))

      change(
        set_attribute(
          :original_registration_id,
          &GenerateCorrelationId.generate/0
        )
      )

      change(set_attribute(:channel_uri, arg(:channel_uri)))
      change(set_attribute(:stream_id, arg(:stream_id)))
      change(set_attribute(:channel_status, :active))
      change(set_attribute(:client_id, arg(:client_id)))
      change(set_attribute(:is_exclusive, arg(:is_exclusive)))
      change(AeronElixir.Changes.InitPublication)
    end

    update :publish do
      require_atomic?(false)
      argument(:message, :string)
      argument(:reserved_value, :integer, default: 0)

      change(AeronElixir.Changes.PublishMessage)
    end

    update :close do
      accept([])
      require_atomic?(false)
      change(set_attribute(:is_closed, true))
      change(set_attribute(:is_connected, false))
      change(AeronElixir.Changes.ClosePublication)
    end
  end

  code_interface do
    define(:add_publication,
      args: [:client_id, :channel_uri, :stream_id, {:optional, :is_exclusive}]
    )

    define(:publish, args: [:message, {:optional, :reserved_value}])
    define(:close, args: [])
    define(:read)
  end
end
