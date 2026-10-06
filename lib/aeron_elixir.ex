defmodule AeronElixir do
  @moduledoc """
  Aeron messaging client for Elixir.

  Provides Ash-based resources for managing Aeron clients, publications,
  subscriptions, images, and counters with native Elixir transport support.

  A started `AeronElixir` process owns a connected `AeronElixir.Resources.Client`
  record. Publication and subscription helpers accept either the owning process
  (or its registered name) or a `Client` struct; the process form is resolved to
  the underlying client before the corresponding Ash action is invoked.

  ## Losing the media driver

  A client follows the Aeron client contract when its media driver goes away:
  when the driver shuts down, stops heartbeating for `driver_timeout_ms`, or
  reports that the client timed out, every publication and subscription of that
  client is closed. Publishing returns `{:error, :closed}`, polling reads nothing,
  `closed?/1` on a handle returns `true`, and `unavailable_image` handlers run for
  the images that were open. The client is not reconnected in place.

  The process started with `start_link/1` exits with the same reason
  (`{:shutdown, :driver_shutdown | :driver_timeout | :client_timeout}`); it exits
  normally when its client is closed with `close/1` or stopped by the
  application. Under a supervisor it is started again and connects a new client,
  waiting up to `driver_timeout_ms` for a driver. Processes that own publications or
  subscriptions belong after it in a `:rest_for_one` supervisor, so they restart
  with it and add their publications and subscriptions again:

      children = [
        {AeronElixir, name: MyApp.Aeron},
        MyApp.OrderPublisher
      ]

      Supervisor.start_link(children, strategy: :rest_for_one)
  """

  use GenServer

  require Logger

  alias AeronElixir.ClientConductor
  alias AeronElixir.DriverConfig
  alias AeronElixir.Header
  alias AeronElixir.ImageCache
  alias AeronElixir.LogBuffer.Publisher
  alias AeronElixir.LogBuffer.Subscriber
  alias AeronElixir.Resources.Client
  alias AeronElixir.Resources.Counter
  alias AeronElixir.Resources.Publication
  alias AeronElixir.Resources.Subscription
  alias AeronElixir.RuntimeHandles
  alias AeronElixir.TransportPublisher
  alias AeronElixir.TransportSubscriber

  @type client_ref :: GenServer.server() | Client.t()

  @doc """
  Connects a client to the media driver and starts the process that owns it,
  registered under `:name` (default `AeronElixir`).

  The driver is the one at `:aeron_directory` when given, otherwise the one
  `AeronElixir.DriverConfig` selects: the application's embedded driver unless
  `AERON_DIR` or `config :aeron_elixir, :aeron_dir` names another. Connecting
  waits up to `:driver_timeout_ms` (default 5000) for that driver to be live, and
  the client is lost once the driver's heartbeat is older than that.

  The client lives as long as the process: when the process exits, for any
  reason, the client is closed and its publications and subscriptions with it.
  If the client is closed or lost first, the process exits too.
  """
  @spec start_link(keyword()) :: GenServer.on_start() | {:error, term()}
  def start_link(opts \\ []) do
    aeron_directory = Keyword.get_lazy(opts, :aeron_directory, &DriverConfig.aeron_dir/0)
    name = Keyword.get(opts, :name, __MODULE__)

    connect_input =
      opts
      |> Keyword.take([:driver_timeout_ms])
      |> Map.new()
      |> Map.put(:aeron_directory, aeron_directory)

    with {:ok, client} <- Client.connect(connect_input) do
      GenServer.start_link(__MODULE__, client, name: name)
    end
  end

  @impl true
  def init(client) do
    Process.flag(:trap_exit, true)
    [{conductor, _value}] = Registry.lookup(AeronElixir.Registry, {:client_conductor, client.id})
    Process.monitor(conductor)
    :ok = ClientConductor.monitor_owner(client.id, self())
    {:ok, client}
  end

  @impl true
  def terminate(_reason, client), do: client |> Client.close() |> report_close(client)

  defp report_close({:ok, _closed}, _client), do: :ok

  defp report_close({:error, reason}, client),
    do: Logger.warning("Aeron client #{client.id} could not be closed: #{inspect(reason)}")

  @impl true
  def handle_call(:client, _from, client) do
    {:reply, client, client}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, _conductor, reason}, client) do
    {:stop, owner_exit_reason(reason), client}
  end

  def handle_info({:EXIT, _pid, reason}, client), do: {:stop, reason, client}

  @doc """
  Returns the number of log and CnC regions currently mapped by this VM.

  A mapping is released once every term referencing it has been collected, so
  this gauge falls after the publications and subscriptions holding a region are
  closed and their handles go out of scope.
  """
  @spec mapped_region_count() :: non_neg_integer()
  defdelegate mapped_region_count(), to: AeronElixir.NIF

  @doc """
  Returns the `Client` record owned by a process started with `start_link/1`.
  """
  @spec client(GenServer.server()) :: Client.t()
  def client(server) do
    GenServer.call(server, :client)
  end

  @doc """
  Connects a client without starting an owning process; `input` takes the same
  keys as the `Client` connect action (`:aeron_directory`, `:driver_timeout_ms`).
  """
  @spec connect(map() | keyword()) :: {:ok, Client.t()} | {:error, term()}
  def connect(input \\ %{}) do
    Client.connect(Map.new(input))
  end

  @doc """
  Adds a publication on `channel_uri` / `stream_id` and waits for the driver to
  assign its session and log buffer. `is_exclusive: true` requests an exclusive
  publication.
  """
  @spec add_publication(client_ref(), String.t(), integer(), keyword()) ::
          {:ok, Publication.t()} | {:error, term()}
  def add_publication(client_or_server, channel_uri, stream_id, opts \\ []) do
    client = resolve_client(client_or_server)

    Publication.add_publication(
      client.id,
      channel_uri,
      stream_id,
      Keyword.get(opts, :is_exclusive, false)
    )
  end

  @doc """
  Adds an exclusive publication: a publication with its own session that only
  this client appends to, the lower-overhead choice when a stream has a single
  publisher.
  """
  @spec add_exclusive_publication(client_ref(), String.t(), integer()) ::
          {:ok, Publication.t()} | {:error, term()}
  def add_exclusive_publication(client_or_server, channel_uri, stream_id) do
    add_publication(client_or_server, channel_uri, stream_id, is_exclusive: true)
  end

  @doc """
  Adds a subscription. Options `:available_image` and `:unavailable_image` take
  1-arity functions invoked with an image summary (`session_id`,
  `correlation_id`, `subscription_registration_id`, `source_identity`) when a
  publisher's image joins or leaves the subscription.
  """
  @spec add_subscription(client_ref(), String.t(), integer(), keyword()) ::
          {:ok, Subscription.t()} | {:error, term()}
  def add_subscription(client_or_server, channel_uri, stream_id, opts \\ []) do
    client = resolve_client(client_or_server)

    Subscription.add_subscription(
      client.id,
      channel_uri,
      stream_id,
      Keyword.get(opts, :available_image),
      Keyword.get(opts, :unavailable_image)
    )
  end

  @doc """
  Allocates a driver counter of `type_id` with the given key bytes and label.
  """
  @spec add_counter(client_ref(), integer(), binary(), String.t()) ::
          {:ok, Counter.t()} | {:error, term()}
  def add_counter(client_or_server, type_id, key, label) do
    client = resolve_client(client_or_server)
    Counter.add_counter(client.id, type_id, key, label)
  end

  @doc """
  Atomically adds `delta` to the counter's value.
  """
  @spec increment_counter(Counter.t(), integer()) :: {:ok, Counter.t()} | {:error, term()}
  def increment_counter(counter, delta \\ 1) do
    Counter.increment(counter, delta)
  end

  @doc """
  Reads the counter's current value from the counters file.
  """
  @spec counter_value(Counter.t()) :: {:ok, integer()} | {:error, term()}
  def counter_value(%Counter{} = counter) do
    ClientConductor.counter_value(counter.client_id, counter.value_address)
  end

  @doc """
  Publishes `message` (a binary or iodata) on `publication` from the calling
  process.

  Appends directly into the mapped log buffer; the conductor is not involved and
  iodata segments are written straight into the frame without flattening. A
  back-pressured publication is retried until the client's publish deadline.
  Safe to call from many processes on the same publication.
  """
  @spec publish(Publication.t() | Publisher.t(), iodata()) :: {:ok, integer()} | {:error, term()}
  def publish(%Publication{} = publication, message)
      when is_binary(message) or is_list(message) do
    TransportPublisher.publish(publication, message, 0)
  end

  def publish(%Publisher{} = handle, message) when is_binary(message) or is_list(message) do
    Publisher.publish(handle, message, 0)
  end

  @doc """
  Makes a single publish attempt and returns the offer status without retrying:
  `{:ok, position}`, or `{:error, :back_pressured | :not_connected | :admin_action
  | :max_position_exceeded | :closed}`.
  """
  @spec try_publish(Publication.t() | Publisher.t(), iodata()) ::
          {:ok, integer()} | {:error, term()}
  def try_publish(%Publication{} = publication, message)
      when is_binary(message) or is_list(message) do
    TransportPublisher.try_publish(publication, message, 0)
  end

  def try_publish(%Publisher{} = handle, message) when is_binary(message) or is_list(message) do
    Publisher.publish(handle, message, 0)
  end

  @doc """
  Returns whether a publication has at least one connected subscriber, or a
  subscription has at least one image. A closed resource is never connected.
  """
  @spec connected?(Publication.t() | Publisher.t() | Subscription.t()) :: boolean()
  def connected?(%Publication{client_id: client_id, registration_id: registration_id}) do
    handle_connected?(TransportPublisher.fetch_handle(client_id, registration_id))
  end

  def connected?(%Publisher{} = handle), do: Publisher.connected?(handle)
  def connected?(%Subscription{} = subscription), do: image_count(subscription) > 0

  defp handle_connected?({:ok, handle}), do: Publisher.connected?(handle)
  defp handle_connected?({:error, _reason}), do: false

  @doc """
  Waits until `connected?/1` holds, polling every millisecond, and returns `:ok`
  or `{:error, :timeout}` after `timeout_ms`.
  """
  @spec await_connected(Publication.t() | Subscription.t(), non_neg_integer()) ::
          :ok | {:error, :timeout}
  def await_connected(resource, timeout_ms \\ 5_000) do
    await_until(fn -> connected?(resource) end, System.monotonic_time(:millisecond) + timeout_ms)
  end

  @doc """
  Returns the publication's current stream position.
  """
  @spec position(Publication.t() | Publisher.t()) :: integer() | {:error, term()}
  def position(%Publication{client_id: client_id, registration_id: registration_id}) do
    with {:ok, handle} <- TransportPublisher.fetch_handle(client_id, registration_id) do
      Publisher.position(handle)
    end
  end

  def position(%Publisher{} = handle), do: Publisher.position(handle)

  @doc """
  Returns the position up to which the publication may append before it is
  back-pressured.
  """
  @spec position_limit(Publication.t() | Publisher.t()) :: integer() | {:error, term()}
  def position_limit(%Publication{client_id: client_id, registration_id: registration_id}) do
    with {:ok, handle} <- TransportPublisher.fetch_handle(client_id, registration_id) do
      Publisher.position_limit(handle)
    end
  end

  def position_limit(%Publisher{} = handle), do: Publisher.position_limit(handle)

  @doc """
  Returns whether the record has been closed with `close/1`, or whether a
  publication or image handle has been closed, by `close/1` or because its client
  lost its media driver.
  """
  @spec closed?(
          Publication.t()
          | Subscription.t()
          | Client.t()
          | Counter.t()
          | Publisher.t()
          | Subscriber.t()
        ) ::
          boolean()
  def closed?(%Publisher{} = handle), do: Publisher.closed?(handle)
  def closed?(%Subscriber{} = image), do: Subscriber.closed?(image)
  def closed?(%Client{status: status}), do: status == :closed
  def closed?(%{is_closed: closed}), do: closed == true

  @doc """
  Publishes `count` copies of `message` in a single batched call.

  Returns `{:ok, published}` where `published` is the number appended before the
  publication back-pressured (`count` on full success); retry the remainder after
  draining the subscriber. The batch is one native `offer_n` call, so the
  NIF-boundary cost is amortized across the whole batch.
  """
  @spec publish_n(Publication.t() | Publisher.t(), binary(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def publish_n(%Publication{} = publication, message, count) do
    TransportPublisher.publish_n(publication, message, 0, count)
  end

  def publish_n(%Publisher{} = handle, message, count) do
    handle |> Publisher.publish_n(message, 0, count) |> appended_result()
  end

  @doc """
  Publishes each element of `payloads` (binaries or iodata) as its own message,
  in order, in one native call.

  Returns `{:ok, published}` where `published` is the number appended before the
  publication back-pressured (`length(payloads)` on full success); resume from
  `Enum.drop(payloads, published)` after the subscriber has drained. This is the
  amortized send path: one NIF crossing per list instead of one per message.
  """
  @spec publish_list(Publication.t() | Publisher.t(), [iodata()]) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def publish_list(%Publication{} = publication, payloads) when is_list(payloads) do
    TransportPublisher.publish_list(publication, payloads, 0)
  end

  def publish_list(%Publisher{} = handle, payloads) when is_list(payloads) do
    handle |> Publisher.publish_list(payloads, 0) |> appended_result()
  end

  defp appended_result({:error, :closed} = closed), do: closed
  defp appended_result(appended), do: {:ok, appended}

  @doc """
  Returns the direct publish handle for `publication`.

  The handle (`%LogBuffer.Publisher{}`) holds the mapped buffer addresses and the
  encoded geometry, and can be used from any process via `publish/2` and
  `publish_n/3` without the per-call handle lookup. Many processes may publish
  through the same handle concurrently.
  """
  @spec publication_handle(Publication.t()) :: {:ok, Publisher.t()} | {:error, term()}
  def publication_handle(%Publication{client_id: client_id, registration_id: registration_id}) do
    RuntimeHandles.fetch_publication(client_id, registration_id)
  end

  @doc """
  Reads up to `fragment_limit` whole messages from every image of `subscription`
  and calls `handler.(payload, header)` for each, in stream order. `header` is an
  encoded `AeronElixir.Header`: read a field with `AeronElixir.Header.session_id/1`
  and the other `AeronElixir.Header` functions, or with a binary match, only when
  the handler needs it. Returns the number of messages delivered, or
  `{:error, reason}` when the subscription is unknown. Only one process may poll
  a subscription at a time.

      AeronElixir.poll(subscription, 10, fn payload, header ->
        IO.inspect({AeronElixir.Header.session_id(header), payload})
      end)
  """
  @spec poll(Subscription.t(), pos_integer(), (binary(), Header.t() -> term())) ::
          non_neg_integer() | {:error, term()}
  def poll(subscription, fragment_limit \\ 10, handler)

  def poll(%Subscription{} = subscription, fragment_limit, handler) do
    subscription
    |> TransportSubscriber.poll(fragment_limit, handler)
    |> unwrap_count()
  end

  defp unwrap_count({:ok, count}), do: count
  defp unwrap_count({:error, _reason} = error), do: error

  @doc """
  Polls like `poll/3` but honours the handler's return value: `:continue`,
  `:commit` (release the position at once), `:break` (stop after this message)
  or `:abort` (stop before it, so it is delivered again). Returns the number of
  messages consumed across the subscription's images.
  """
  @spec controlled_poll(
          Subscription.t(),
          pos_integer(),
          (binary(), Header.t() -> Subscriber.poll_action())
        ) ::
          non_neg_integer() | {:error, term()}
  def controlled_poll(subscription, fragment_limit \\ 10, handler)

  def controlled_poll(%Subscription{} = subscription, fragment_limit, handler) do
    with {:ok, images} <- ImageCache.images(subscription) do
      Enum.reduce(images, 0, &(Subscriber.controlled_poll(&1, fragment_limit, handler) + &2))
    end
  end

  @doc """
  Returns the images (one per publisher session) currently attached to the
  subscription, as `AeronElixir.LogBuffer.Subscriber` handles.
  """
  @spec images(Subscription.t()) :: [Subscriber.t()]
  def images(%Subscription{client_id: client_id, registration_id: registration_id}) do
    known_images(RuntimeHandles.images(client_id, registration_id))
  end

  defp known_images({:ok, images}), do: images
  defp known_images({:error, _reason}), do: []

  @doc """
  Returns the number of images (publisher sessions) currently on `subscription`.
  """
  @spec image_count(Subscription.t()) :: non_neg_integer()
  def image_count(%Subscription{} = subscription), do: length(images(subscription))

  @doc """
  Finds the image for a publisher `session_id` on `subscription`.
  """
  @spec image_by_session_id(Subscription.t(), integer()) ::
          {:ok, Subscriber.t()} | {:error, :unknown_session}
  def image_by_session_id(%Subscription{} = subscription, session_id) do
    subscription
    |> images()
    |> Enum.find(&(&1.session_id == session_id))
    |> found_image()
  end

  defp found_image(nil), do: {:error, :unknown_session}
  defp found_image(%Subscriber{} = image), do: {:ok, image}

  @doc """
  Polls one image with a handler, like `poll/3` for a single publisher session.
  """
  @spec poll_image(Subscriber.t(), pos_integer(), (binary(), Header.t() -> term())) ::
          non_neg_integer()
  def poll_image(%Subscriber{} = image, fragment_limit \\ 10, handler) do
    Subscriber.poll(image, fragment_limit, handler)
  end

  @doc """
  Reads the next whole message from one image and returns `{:ok, payload}`, or
  `:empty` when no complete message is waiting or the image is closed.

  The payload is a binary of its own. This is the cheapest way to read one
  message at a time; `poll_image_batch/2` reads many in one call.
  """
  @spec poll_image_next(Subscriber.t()) :: {:ok, binary()} | :empty
  def poll_image_next(%Subscriber{} = image), do: image |> Subscriber.next() |> next_result()

  defp next_result(nil), do: :empty
  defp next_result(payload), do: {:ok, payload}

  @doc """
  Polls one image and returns `{:ok, count, payloads}`, like `poll_batch/2`.
  """
  @spec poll_image_batch(Subscriber.t(), pos_integer()) :: {:ok, non_neg_integer(), [binary()]}
  def poll_image_batch(%Subscriber{} = image, fragment_limit \\ 64) do
    {count, payloads} = Subscriber.collect(image, fragment_limit)
    {:ok, count, payloads}
  end

  @doc """
  Returns the subscriber position of `image`: the stream position of the next
  byte to be read.
  """
  @spec image_position(Subscriber.t()) :: integer()
  def image_position(%Subscriber{} = image), do: Subscriber.position(image)

  @doc """
  Returns whether the publisher of `image` has ended its stream and the image has
  been read up to that point.
  """
  @spec end_of_stream?(Subscriber.t()) :: boolean()
  def end_of_stream?(%Subscriber{} = image), do: Subscriber.end_of_stream?(image)

  @doc """
  Polls up to `message_limit` whole messages and returns their payloads as a batch.

  Returns `{:ok, count, payloads}` where `payloads` is a list of complete message
  binaries. The scan, multi-fragment reassembly, copy and batching all happen in a
  single native call, so this is the fast consume path for any payload size (unlike
  `poll/3` it does not invoke a per-fragment handler). A message whose fragments are
  not yet fully published is left for a subsequent poll.
  """
  @spec poll_batch(Subscription.t(), pos_integer()) ::
          {:ok, non_neg_integer(), [binary()]} | {:error, term()}
  def poll_batch(subscription, fragment_limit \\ 64)

  def poll_batch(%Subscription{} = subscription, fragment_limit) do
    with {:ok, images} <- ImageCache.images(subscription) do
      {count, payloads} =
        Enum.reduce(images, {0, []}, fn image, {acc_count, acc_payloads} ->
          {read, payloads} = Subscriber.collect(image, fragment_limit)
          {acc_count + read, acc_payloads ++ payloads}
        end)

      {:ok, count, payloads}
    end
  end

  def poll_batch(%Subscriber{} = image, fragment_limit) do
    {count, payloads} = Subscriber.collect(image, fragment_limit)
    {:ok, count, payloads}
  end

  @doc """
  Returns the current direct image handles for `subscription`.

  Each handle (`%LogBuffer.Subscriber{}`) can be polled directly via `poll_batch/2`
  from a dedicated owning process with no conductor hop. Re-fetch to pick up images
  that appear after this call (new publishers joining the stream).
  """
  @spec subscription_handles(Subscription.t()) ::
          {:ok, [Subscriber.t()]} | {:error, :closed}
  def subscription_handles(%Subscription{client_id: client_id, registration_id: registration_id}) do
    RuntimeHandles.images(client_id, registration_id)
  end

  @doc """
  Adds a destination to a publication or subscription whose channel is in manual
  control mode (`aeron:udp?control-mode=manual`).

  A manual-control publication sends each message to every destination added to
  it; a manual-control subscription receives from every destination added to it.
  """
  @spec add_destination(Publication.t() | Subscription.t(), String.t()) :: :ok | {:error, term()}
  def add_destination(
        %Publication{client_id: client_id, registration_id: registration_id},
        destination
      )
      when is_binary(destination) do
    ClientConductor.add_publication_destination(client_id, registration_id, destination)
  end

  def add_destination(
        %Subscription{client_id: client_id, registration_id: registration_id},
        destination
      )
      when is_binary(destination) do
    ClientConductor.add_subscription_destination(client_id, registration_id, destination)
  end

  @doc """
  Removes a destination previously added with `add_destination/2`.
  """
  @spec remove_destination(Publication.t() | Subscription.t(), String.t()) ::
          :ok | {:error, term()}
  def remove_destination(
        %Publication{client_id: client_id, registration_id: registration_id},
        destination
      )
      when is_binary(destination) do
    ClientConductor.remove_publication_destination(client_id, registration_id, destination)
  end

  def remove_destination(
        %Subscription{client_id: client_id, registration_id: registration_id},
        destination
      )
      when is_binary(destination) do
    ClientConductor.remove_subscription_destination(client_id, registration_id, destination)
  end

  @doc """
  Closes a client, publication, subscription or counter, releasing its driver
  resources. Returns the closed record.
  """
  @spec close(Client.t() | Publication.t() | Subscription.t() | Counter.t()) ::
          {:ok, struct()} | {:error, term()}
  def close(%Client{} = client), do: Client.close(client)
  def close(%Publication{} = publication), do: Publication.close(publication)
  def close(%Subscription{} = subscription), do: Subscription.close(subscription)
  def close(%Counter{} = counter), do: Counter.close(counter)
  def close(_resource), do: {:error, :unknown_resource_type}

  defp await_until(condition, deadline) do
    await_result(condition.(), condition, deadline, System.monotonic_time(:millisecond))
  end

  defp await_result(true, _condition, _deadline, _now), do: :ok
  defp await_result(false, _condition, deadline, now) when now >= deadline, do: {:error, :timeout}

  defp await_result(false, condition, deadline, _now) do
    Process.sleep(1)
    await_until(condition, deadline)
  end

  defp owner_exit_reason(reason) when reason in [:normal, :shutdown], do: :normal
  defp owner_exit_reason(reason), do: reason

  defp resolve_client(%Client{} = client), do: client
  defp resolve_client(server), do: client(server)
end
