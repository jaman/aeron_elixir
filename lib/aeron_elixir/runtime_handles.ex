defmodule AeronElixir.RuntimeHandles do
  @moduledoc """
  Lookup table of the runtime publication and image handles a client conductor has
  mapped.

  Each conductor owns one public ETS table, created with `create/2` and found through
  `AeronElixir.Registry` under `{:runtime_handles, client_record_id}`. The conductor
  writes handles as the driver assigns them and removes them on close. Any process reads
  them with `fetch_publication/2` and `images/2` and drives the log buffers directly
  through `AeronElixir.LogBuffer.Publisher` and `AeronElixir.LogBuffer.Subscriber`,
  without sending a message to the conductor.

  The table dies with its conductor, so a handle read from it is valid until the
  conductor closes the publication or subscription it belongs to. A read that
  races with the conductor stopping returns `{:error, :closed}`, whether the
  registration or the table disappeared first.

  Reads return `{:error, :closed}` when no conductor is registered for the record
  id, because its client was closed or lost its media driver, and
  `{:error, :unknown_publication}` for a registration id the conductor has not
  mapped or has already closed.
  """

  alias AeronElixir.LogBuffer.Publisher
  alias AeronElixir.LogBuffer.Subscriber

  @type table :: :ets.tid()

  @doc """
  Creates the calling conductor's table and registers it for `client_record_id`.

  `publish_timeout_ms` bounds how long `publish_deadline/1` lets a back-pressured
  publish retry.
  """
  @spec create(term(), pos_integer()) :: table()
  def create(client_record_id, publish_timeout_ms) do
    table = :ets.new(__MODULE__, [:ordered_set, :public, read_concurrency: true])
    true = :ets.insert(table, {:publish_timeout_ms, publish_timeout_ms})
    true = :ets.insert(table, {:image_version, :atomics.new(1, signed: false)})

    {:ok, _owner} =
      Registry.register(AeronElixir.Registry, {:runtime_handles, client_record_id}, table)

    table
  end

  @doc """
  Stores a publication handle under its registration id.
  """
  @spec put_publication(table(), Publisher.t()) :: :ok
  def put_publication(table, %Publisher{registration_id: registration_id} = publication) do
    true = :ets.insert(table, {{:publication, registration_id}, publication})
    :ok
  end

  @doc """
  Removes the publication handle with `registration_id`.
  """
  @spec delete_publication(table(), integer()) :: :ok
  def delete_publication(table, registration_id) do
    true = :ets.delete(table, {:publication, registration_id})
    :ok
  end

  @doc """
  Stores an image handle under its subscription registration id and correlation id.
  """
  @spec put_image(table(), Subscriber.t()) :: :ok
  def put_image(
        table,
        %Subscriber{subscription_registration_id: subscription_id, correlation_id: correlation_id} =
          image
      ) do
    true = :ets.insert(table, {{:image, subscription_id, correlation_id}, image})
    advance_image_version(table)
  end

  @doc """
  Removes one image handle.
  """
  @spec delete_image(table(), integer(), integer()) :: :ok
  def delete_image(table, subscription_registration_id, correlation_id) do
    true = :ets.delete(table, {:image, subscription_registration_id, correlation_id})
    advance_image_version(table)
  end

  @doc """
  Removes every image handle of a subscription.
  """
  @spec delete_subscription(table(), integer()) :: :ok
  def delete_subscription(table, subscription_registration_id) do
    true = :ets.match_delete(table, {{:image, subscription_registration_id, :_}, :_})
    advance_image_version(table)
  end

  @doc """
  Returns the client's image version: a one-element `:atomics` array whose value
  rises every time an image is stored or removed on any of the client's
  subscriptions, and when the client stops. `AeronElixir.ImageCache` compares it
  to decide whether its cached images are still current.
  """
  @spec image_version(table()) :: :atomics.atomics_ref()
  def image_version(table) do
    [{:image_version, version}] = :ets.lookup(table, :image_version)
    version
  end

  @doc """
  Raises the image version, so every cached image list for the client is read
  again. The conductor calls it when it stops, before its table goes away.
  """
  @spec advance_image_version(table()) :: :ok
  def advance_image_version(table), do: table |> image_version() |> :atomics.add(1, 1)

  @doc """
  Looks up the publication handle for `registration_id` on the client with `client_record_id`.
  """
  @spec fetch_publication(term(), integer()) ::
          {:ok, Publisher.t()} | {:error, :closed | :unknown_publication}
  def fetch_publication(client_record_id, registration_id) do
    with {:ok, table} <- lookup_table(client_record_id),
         {:ok, entries} <- table_lookup(table, {:publication, registration_id}) do
      publication_entry(entries)
    end
  end

  defp publication_entry([{_key, publication}]), do: {:ok, publication}
  defp publication_entry([]), do: {:error, :unknown_publication}

  @doc """
  Looks up the image handle with `correlation_id` on the client with `client_record_id`.
  """
  @spec fetch_image(term(), integer()) ::
          {:ok, Subscriber.t()} | {:error, :closed | :unknown_image}
  def fetch_image(client_record_id, correlation_id) do
    pattern = {{:image, :_, correlation_id}, :"$1"}

    with {:ok, table} <- lookup_table(client_record_id),
         {:ok, selection} <- table_select(table, [{pattern, [], [:"$1"]}], 1) do
      image_selection(selection)
    end
  end

  defp image_selection({[image], _continuation}), do: {:ok, image}
  defp image_selection(:"$end_of_table"), do: {:error, :unknown_image}

  @doc """
  Returns the images currently mapped for a subscription, in session order.
  """
  @spec images(term(), integer()) :: {:ok, [Subscriber.t()]} | {:error, :closed}
  def images(client_record_id, subscription_registration_id) do
    pattern = {{:image, subscription_registration_id, :_}, :"$1"}

    with {:ok, table} <- lookup_table(client_record_id) do
      table_select(table, [{pattern, [], [:"$1"]}])
    end
  end

  @doc """
  Returns the monotonic millisecond deadline by which a back-pressured publish on
  this client must succeed before `AeronElixir.publish/2` gives up.
  """
  @spec publish_deadline(term()) :: {:ok, integer()} | {:error, :closed}
  def publish_deadline(client_record_id) do
    with {:ok, table} <- lookup_table(client_record_id),
         {:ok, [{:publish_timeout_ms, timeout_ms}]} <- table_lookup(table, :publish_timeout_ms) do
      {:ok, System.monotonic_time(:millisecond) + timeout_ms}
    end
  end

  defp lookup_table(client_record_id) do
    registered_table(Registry.lookup(AeronElixir.Registry, {:runtime_handles, client_record_id}))
  end

  defp registered_table([{_owner, table}]), do: {:ok, table}
  defp registered_table([]), do: {:error, :closed}

  defp table_lookup(table, key) do
    {:ok, :ets.lookup(table, key)}
  rescue
    ArgumentError -> {:error, :closed}
  end

  defp table_select(table, match_spec) do
    {:ok, :ets.select(table, match_spec)}
  rescue
    ArgumentError -> {:error, :closed}
  end

  defp table_select(table, match_spec, limit) do
    {:ok, :ets.select(table, match_spec, limit)}
  rescue
    ArgumentError -> {:error, :closed}
  end
end
