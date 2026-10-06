defmodule AeronElixir.Resources.Image do
  @moduledoc """
  Ash resource for Aeron images.

  An image represents a replicated publication stream from a single publisher.
  """

  use Ash.Resource,
    domain: AeronElixir.Domain,
    data_layer: Ash.DataLayer.Ets

  attributes do
    uuid_primary_key(:id)

    attribute :correlation_id, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :session_id, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :source_identity, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :join_position, :integer do
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

    attribute :mtu_length, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :initial_term_id, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :subscriber_position_id, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :subscriber_position, :integer do
      default(0)
      public?(true)
    end

    attribute :end_of_stream_position, :integer do
      default(-1)
      public?(true)
    end

    attribute :is_closed, :boolean do
      default(false)
      public?(true)
    end

    attribute :is_end_of_stream, :boolean do
      default(false)
      public?(true)
    end

    attribute :active_transport_count, :integer do
      default(0)
      public?(true)
    end

    attribute :is_publication_revoked, :boolean do
      default(false)
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
    belongs_to :subscription, AeronElixir.Resources.Subscription do
      allow_nil?(false)
    end
  end

  actions do
    defaults([:read, :update, :destroy])

    update :poll do
      require_atomic?(false)
      argument(:fragment_limit, :integer, default: 10)
      argument(:fragment_handler, :term)
      argument(:limit_position, :integer, default: nil)

      change(AeronElixir.Changes.PollImage)
    end

    update :controlled_poll do
      require_atomic?(false)
      argument(:fragment_limit, :integer, default: 10)
      argument(:fragment_handler, :term)
      argument(:limit_position, :integer, default: nil)

      change(AeronElixir.Changes.ControlledPollImage)
    end

    update :set_position do
      argument(:position, :integer)

      change(set_attribute(:subscriber_position, arg(:position)))
    end

    update :reject do
      require_atomic?(false)
      argument(:reason, :string)

      change(set_attribute(:is_closed, true))
      change(AeronElixir.Changes.RejectImage)
    end
  end

  code_interface do
    define(:poll)
    define(:controlled_poll)
    define(:set_position)
    define(:reject)
    define(:read)
  end
end
