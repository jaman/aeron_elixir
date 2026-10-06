defmodule AeronElixir.ClientConductor do
  @moduledoc """
  Client-side conductor that performs the Aeron driver handshake.

  On start it maps the CnC file, verifies driver liveness, allocates a client id
  from the to-driver ring buffer's correlation counter and seeds its broadcast
  cursor from the current to-clients tail. `add_publication/3` and
  `add_subscription/3` encode the matching control command, write it to the
  to-driver ring buffer, then drain the to-clients broadcast buffer until the
  driver's `on_publication_ready` / `on_subscription_ready` (or `on_error`) for the
  request's correlation id arrives, mapping the returned log file and reading its
  descriptor to produce the driver-assigned geometry.

  Starting waits up to `driver_timeout_ms` for a live driver to serve the CnC
  file. While running, every keepalive interval it checks the driver's heartbeat.
  The client is lost when the driver has shut down, when its heartbeat is older
  than `driver_timeout_ms`, or when the driver reports that this client timed
  out. The conductor then stops with `{:shutdown, :driver_shutdown}`,
  `{:shutdown, :driver_timeout}` or `{:shutdown, :client_timeout}`.

  Whenever the conductor stops, it closes every publication and image it holds:
  publishing returns `{:error, :closed}`, polling reads nothing, and each
  subscription's `unavailable` image handler is called for its images. Calls to a
  conductor that is no longer running return `{:error, :closed}`. A conductor is
  never restarted with the same client; a new client must be connected.
  """

  use GenServer

  require Logger

  alias AeronElixir.Broadcast.Receiver
  alias AeronElixir.CnC.Reader
  alias AeronElixir.CounterManager
  alias AeronElixir.LogBuffer.Descriptor
  alias AeronElixir.LogBuffer.Mapping
  alias AeronElixir.LogBuffer.Publisher
  alias AeronElixir.LogBuffer.Subscriber
  alias AeronElixir.NIF
  alias AeronElixir.Protocol.Control
  alias AeronElixir.RuntimeHandles

  @poll_interval_ms 1
  @default_request_timeout_ms 5000
  @conductor_tick_interval_ms 10
  @keepalive_interval_ms 500

  defstruct [
    :client_id,
    :record_id,
    :cnc,
    :driver_timeout_ms,
    :broadcast_position,
    :handles,
    :aeron_directory,
    :owner_monitor,
    last_keepalive_ms: 0,
    client_timed_out?: false,
    publications: %{},
    subscriptions: %{},
    images: %{}
  ]

  @type image_handlers :: %{
          optional(:available) => (map() -> term()) | nil,
          optional(:unavailable) => (map() -> term()) | nil
        }

  @spec start_link(map()) :: GenServer.on_start()
  def start_link(client) do
    GenServer.start_link(__MODULE__, client, name: via_tuple(client.id))
  end

  @doc """
  Makes the conductor stop, closing its client, if `owner` exits without closing
  it first. `AeronElixir.start_link/1` registers the process it starts.
  """
  @spec monitor_owner(term(), pid()) :: :ok | {:error, :closed}
  def monitor_owner(record_id, owner),
    do: call(record_id, {:monitor_owner, owner}, @default_request_timeout_ms)

  @doc """
  Registers a publication (exclusive when `exclusive?`) with the driver and returns the driver-assigned fields once the log is mapped.
  """
  @spec add_publication(term(), String.t(), integer(), boolean()) ::
          {:ok, map()} | {:error, term()}
  def add_publication(record_id, channel, stream_id, exclusive? \\ false) do
    call(record_id, {:add_publication, channel, stream_id, exclusive?}, :infinity)
  end

  @doc """
  Registers a subscription with the driver. `handlers` holds optional
  `:available` and `:unavailable` 1-arity functions the conductor calls with an
  image summary (`session_id`, `correlation_id`, `subscription_registration_id`,
  `source_identity`) as images come and go.
  """
  @spec add_subscription(term(), String.t(), integer(), image_handlers()) ::
          {:ok, map()} | {:error, term()}
  def add_subscription(record_id, channel, stream_id, handlers \\ %{}) do
    call(record_id, {:add_subscription, channel, stream_id, handlers}, :infinity)
  end

  @doc """
  Allocates a driver counter and returns its id and value address.
  """
  @spec add_counter(term(), integer(), binary(), String.t()) :: {:ok, map()} | {:error, term()}
  def add_counter(record_id, type_id, key, label) do
    call(record_id, {:add_counter, type_id, key, label}, :infinity)
  end

  @doc """
  Atomically adds `delta` to the counter at `value_address` and returns the new value.
  """
  @spec increment_counter(term(), integer(), integer()) :: {:ok, integer()} | {:error, term()}
  def increment_counter(record_id, value_address, delta) do
    call(record_id, {:increment_counter, value_address, delta}, 5_000)
  end

  @doc """
  Reads the counter at `value_address`.
  """
  @spec counter_value(term(), integer()) :: {:ok, integer()} | {:error, term()}
  def counter_value(record_id, value_address) do
    call(record_id, {:counter_value, value_address}, 5_000)
  end

  @doc """
  Asks the driver to free the counter with `registration_id`.
  """
  @spec remove_counter(term(), integer()) :: :ok | {:error, term()}
  def remove_counter(record_id, registration_id) do
    call(record_id, {:remove_counter, registration_id}, :infinity)
  end

  @doc """
  Returns `:connected` while the conductor is running and `:closed` once it has
  stopped.
  """
  @spec status(term()) :: :connected | :closed
  def status(record_id) do
    record_id
    |> call(:status, 5_000)
    |> status_from_reply()
  end

  defp status_from_reply({:error, :closed}), do: :closed
  defp status_from_reply(status), do: status

  @doc """
  Adds `destination` to the publication registered as `registration_id`.

  The publication's channel must be in manual control mode
  (`aeron:udp?control-mode=manual`).
  """
  @spec add_publication_destination(term(), integer(), String.t()) :: :ok | {:error, term()}
  def add_publication_destination(client_record_id, registration_id, destination) do
    call(
      client_record_id,
      {:destination, Control.command_add_destination(), registration_id, destination},
      :infinity
    )
  end

  @doc """
  Removes `destination` from the publication registered as `registration_id`.
  """
  @spec remove_publication_destination(term(), integer(), String.t()) :: :ok | {:error, term()}
  def remove_publication_destination(client_record_id, registration_id, destination) do
    call(
      client_record_id,
      {:destination, Control.command_remove_destination(), registration_id, destination},
      :infinity
    )
  end

  @doc """
  Adds `destination` to the subscription registered as `registration_id`.

  The subscription's channel must be in manual control mode.
  """
  @spec add_subscription_destination(term(), integer(), String.t()) :: :ok | {:error, term()}
  def add_subscription_destination(client_record_id, registration_id, destination) do
    call(
      client_record_id,
      {:destination, Control.command_add_rcv_destination(), registration_id, destination},
      :infinity
    )
  end

  @doc """
  Removes `destination` from the subscription registered as `registration_id`.
  """
  @spec remove_subscription_destination(term(), integer(), String.t()) :: :ok | {:error, term()}
  def remove_subscription_destination(client_record_id, registration_id, destination) do
    call(
      client_record_id,
      {:destination, Control.command_remove_rcv_destination(), registration_id, destination},
      :infinity
    )
  end

  @doc """
  Removes the publication with `registration_id` from the driver and drops its runtime handle.
  """
  @spec remove_publication(term(), integer()) :: :ok | {:error, term()}
  def remove_publication(client_record_id, registration_id) do
    call(client_record_id, {:remove_publication, registration_id}, :infinity)
  end

  @doc """
  Drops the image with `correlation_id` from the conductor and the runtime handles.
  """
  @spec remove_image(term(), integer()) :: :ok | {:error, :closed}
  def remove_image(client_record_id, correlation_id) do
    call(client_record_id, {:remove_image, correlation_id}, 5_000)
  end

  @doc """
  Removes the subscription with `subscription_registration_id` from the driver and drops every image it owned.
  """
  @spec remove_subscription(term(), integer()) :: :ok | {:error, term()}
  def remove_subscription(client_record_id, subscription_registration_id) do
    call(client_record_id, {:remove_subscription, subscription_registration_id}, :infinity)
  end

  @doc """
  Returns the broadcast position to read from on the next duty cycle.

  A drain that completed resumes at the position it reached. A drain that was
  lapped cannot trust its cursor, so it resumes at the transmitter's latest
  record; every record written between the stale position and that one is lost.
  """
  @spec next_broadcast_position(
          {:ok, [Receiver.broadcast_record()], integer()} | {:error, :lapped},
          Receiver.t()
        ) :: integer()
  def next_broadcast_position({:ok, _records, next_position}, _receiver), do: next_position

  def next_broadcast_position({:error, :lapped}, receiver),
    do: Receiver.latest_position(receiver)

  defp via_tuple(record_id) do
    {:via, Registry, {AeronElixir.Registry, {:client_conductor, record_id}}}
  end

  defp call(record_id, request, timeout) do
    GenServer.call(via_tuple(record_id), request, timeout)
  catch
    :exit, {:noproc, _call} -> {:error, :closed}
    :exit, {:normal, _call} -> {:error, :closed}
    :exit, {:shutdown, _call} -> {:error, :closed}
    :exit, {{:shutdown, _reason}, _call} -> {:error, :closed}
  end

  @impl true
  def init(%{id: record_id} = client) do
    Process.flag(:trap_exit, true)
    Registry.register(AeronElixir.Registry, {:client_conductor, record_id}, client)
    driver_timeout_ms = Map.get(client, :driver_timeout_ms, @default_request_timeout_ms)

    client.aeron_directory
    |> Reader.await_connect(driver_timeout_ms)
    |> connected_state(client, driver_timeout_ms)
  end

  defp connected_state({:error, reason}, _client, _driver_timeout_ms), do: {:stop, reason}

  defp connected_state({:ok, cnc}, %{id: record_id} = client, driver_timeout_ms) do
    schedule_conductor_tick()

    {:ok,
     %__MODULE__{
       client_id: Reader.next_correlation_id(cnc),
       record_id: record_id,
       cnc: cnc,
       driver_timeout_ms: driver_timeout_ms,
       broadcast_position: Receiver.latest_position(Reader.to_clients(cnc)),
       handles: RuntimeHandles.create(record_id, driver_timeout_ms),
       aeron_directory: client.aeron_directory
     }}
  end

  @impl true
  def handle_info(:conductor_tick, state) do
    schedule_conductor_tick()

    state
    |> drain_available_images()
    |> service_driver_link(now_ms())
  end

  def handle_info({:DOWN, ref, :process, _owner, _reason}, %{owner_monitor: ref} = state),
    do: {:stop, :shutdown, state}

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  @impl true
  def terminate(reason, state) do
    force_close(state)
    close_client_on_driver(reason, state)
    report_driver_loss(reason, state)
  end

  defp schedule_conductor_tick do
    Process.send_after(self(), :conductor_tick, @conductor_tick_interval_ms)
  end

  defp service_driver_link(%{client_timed_out?: true} = state, _now),
    do: {:stop, {:shutdown, :client_timeout}, state}

  defp service_driver_link(%{last_keepalive_ms: last} = state, now)
       when now - last < @keepalive_interval_ms,
       do: {:noreply, state}

  defp service_driver_link(state, now) do
    state.cnc
    |> Reader.driver_heartbeat_ms()
    |> driver_link_status(now, state.driver_timeout_ms)
    |> keep_driver_link(state, now)
  end

  defp driver_link_status(-1, _now, _driver_timeout_ms), do: :driver_shutdown

  defp driver_link_status(heartbeat_ms, now, driver_timeout_ms)
       when now - heartbeat_ms > driver_timeout_ms,
       do: :driver_timeout

  defp driver_link_status(_heartbeat_ms, _now, _driver_timeout_ms), do: :alive

  defp keep_driver_link(:alive, state, now), do: {:noreply, send_keepalive(state, now)}
  defp keep_driver_link(lost, state, _now), do: {:stop, {:shutdown, lost}, state}

  defp send_keepalive(state, now) do
    command = Control.encode_client_keepalive(state.client_id, state.client_id)
    Reader.write_command(state.cnc, Control.command_client_keepalive(), command)
    %{state | last_keepalive_ms: now}
  end

  defp force_close(state) do
    Enum.each(state.publications, fn {_registration_id, publication} ->
      close_handle(publication)
    end)

    Enum.each(state.images, fn {_key, image} -> close_handle(image) end)
    RuntimeHandles.advance_image_version(state.handles)
    Enum.each(state.images, fn {_key, image} -> notify_unavailable(state, image) end)
  end

  defp close_handle(%{closed_address: address}), do: NIF.write_int32_ordered(address, 1)

  defp notify_unavailable(state, image),
    do: notify_image_handler(state, image.subscription_registration_id, :unavailable, image)

  defp close_client_on_driver(reason, state) when reason in [:normal, :shutdown] do
    command = Control.encode_client_close(state.client_id, Reader.next_correlation_id(state.cnc))
    Reader.write_command(state.cnc, Control.command_client_close(), command)
  end

  defp close_client_on_driver(_reason, _state), do: :ok

  defp report_driver_loss({:shutdown, lost}, state)
       when lost in [:driver_shutdown, :driver_timeout, :client_timeout] do
    Logger.error(
      "Aeron client #{state.client_id} lost its media driver at #{state.aeron_directory} (#{lost}); " <>
        "its publications and subscriptions are closed"
    )
  end

  defp report_driver_loss(_reason, _state), do: :ok

  @impl true
  def handle_call(:status, _from, state), do: {:reply, :connected, state}

  def handle_call({:monitor_owner, owner}, _from, state),
    do: {:reply, :ok, %{state | owner_monitor: Process.monitor(owner)}}

  def handle_call({:add_publication, channel, stream_id, exclusive?}, _from, state) do
    correlation_id = Reader.next_correlation_id(state.cnc)
    command = Control.encode_add_publication(state.client_id, correlation_id, stream_id, channel)

    state
    |> offer_and_await(publication_command_type(exclusive?), command, correlation_id)
    |> reply_add_publication(state, correlation_id)
  end

  def handle_call({:remove_image, correlation_id}, _from, state) do
    {matching, remaining} =
      Enum.split_with(state.images, fn {{_subscription_id, id}, _image} ->
        id == correlation_id
      end)

    for {{subscription_id, id}, _image} <- matching do
      RuntimeHandles.delete_image(state.handles, subscription_id, id)
    end

    {:reply, :ok, %{state | images: Map.new(remaining)}}
  end

  def handle_call({:destination, command_type, registration_id, destination}, _from, state) do
    correlation_id = Reader.next_correlation_id(state.cnc)

    command =
      Control.encode_destination(state.client_id, correlation_id, registration_id, destination)

    state
    |> offer_and_await(command_type, command, correlation_id)
    |> reply_operation(state)
  end

  def handle_call({:remove_publication, registration_id}, _from, state) do
    state.publications |> Map.get(registration_id) |> close_known_handle()
    RuntimeHandles.delete_publication(state.handles, registration_id)
    state = %{state | publications: Map.delete(state.publications, registration_id)}
    remove_from_driver(state, Control.command_remove_publication(), registration_id)
  end

  def handle_call({:remove_subscription, subscription_registration_id}, _from, state) do
    RuntimeHandles.delete_subscription(state.handles, subscription_registration_id)

    {closing, remaining} =
      Enum.split_with(state.images, fn {_id, image} ->
        image.subscription_registration_id == subscription_registration_id
      end)

    Enum.each(closing, fn {_key, image} -> close_handle(image) end)
    Enum.each(closing, fn {_key, image} -> notify_unavailable(state, image) end)
    images = Map.new(remaining)

    subscriptions = Map.delete(state.subscriptions, subscription_registration_id)
    state = %{state | images: images, subscriptions: subscriptions}
    remove_from_driver(state, Control.command_remove_subscription(), subscription_registration_id)
  end

  def handle_call({:add_subscription, channel, stream_id, handlers}, _from, state) do
    correlation_id = Reader.next_correlation_id(state.cnc)

    command =
      Control.encode_add_subscription(
        state.client_id,
        correlation_id,
        Control.registration_id_new(),
        stream_id,
        channel
      )

    state
    |> offer_and_await(Control.command_add_subscription(), command, correlation_id)
    |> reply_add_subscription(state, correlation_id, handlers)
  end

  def handle_call({:add_counter, type_id, key, label}, _from, state) do
    correlation_id = Reader.next_correlation_id(state.cnc)
    command = Control.encode_add_counter(state.client_id, correlation_id, type_id, key, label)

    state
    |> offer_and_await(Control.command_add_counter(), command, correlation_id)
    |> reply_add_counter(state, correlation_id)
  end

  def handle_call({:remove_counter, registration_id}, _from, state) do
    correlation_id = Reader.next_correlation_id(state.cnc)

    command =
      Control.encode_remove_counter(state.client_id, correlation_id, registration_id)

    state
    |> offer_and_await(Control.command_remove_counter(), command, correlation_id)
    |> reply_operation(state)
  end

  def handle_call({:increment_counter, value_address, delta}, _from, state) do
    {:reply, CounterManager.increment(value_address, delta), state}
  end

  def handle_call({:counter_value, value_address}, _from, state) do
    {:reply, CounterManager.get(value_address), state}
  end

  defp publication_command_type(true), do: Control.command_add_exclusive_publication()
  defp publication_command_type(false), do: Control.command_add_publication()

  defp reply_add_publication(
         {:ok, {ready, fields}, position, image_fields},
         state,
         correlation_id
       )
       when ready in [:on_publication_ready, :on_exclusive_publication_ready] do
    register_publication(
      fields,
      correlation_id,
      position,
      apply_driver_events(state, image_fields)
    )
  end

  defp reply_add_publication(result, state, _correlation_id), do: reply_failure(result, state)

  defp reply_failure({:ok, {:on_error, fields}, position, image_fields}, state) do
    {:reply, {:error, {:driver_error, fields.error_code, fields.error_message}},
     apply_driver_events(%{state | broadcast_position: position}, image_fields)}
  end

  defp reply_failure({:error, reason}, state), do: {:reply, {:error, reason}, state}

  defp reply_failure({:error, reason, position}, state),
    do: {:reply, {:error, reason}, %{state | broadcast_position: position}}

  defp reply_add_subscription(
         {:ok, {:on_subscription_ready, fields}, position, image_fields},
         state,
         correlation_id,
         handlers
       ) do
    subscriptions = Map.put(state.subscriptions, correlation_id, handlers)

    {:reply,
     {:ok,
      %{
        registration_id: correlation_id,
        channel_status_indicator_id: fields.channel_status_indicator_id,
        image_version: RuntimeHandles.image_version(state.handles)
      }},
     apply_driver_events(
       %{state | broadcast_position: position, subscriptions: subscriptions},
       image_fields
     )}
  end

  defp reply_add_subscription(result, state, _correlation_id, _handlers),
    do: reply_failure(result, state)

  defp reply_add_counter(
         {:ok, {:on_counter_ready, fields}, position, image_fields},
         state,
         correlation_id
       ) do
    result = %{
      registration_id: correlation_id,
      counter_id: fields.counter_id,
      value_address: Reader.counter_address(state.cnc, fields.counter_id)
    }

    {:reply, {:ok, result},
     apply_driver_events(%{state | broadcast_position: position}, image_fields)}
  end

  defp reply_add_counter(result, state, _correlation_id), do: reply_failure(result, state)

  defp reply_operation({:ok, {:on_operation_success, _fields}, position, image_fields}, state) do
    {:reply, :ok, apply_driver_events(%{state | broadcast_position: position}, image_fields)}
  end

  defp reply_operation(result, state), do: reply_failure(result, state)

  defp remove_from_driver(state, command_type, registration_id) do
    correlation_id = Reader.next_correlation_id(state.cnc)
    command = Control.encode_remove(state.client_id, correlation_id, registration_id)

    state
    |> offer_and_await(command_type, command, correlation_id)
    |> reply_operation(state)
  end

  defp close_known_handle(nil), do: :ok
  defp close_known_handle(handle), do: close_handle(handle)

  defp offer_and_await(state, message_type, command, correlation_id) do
    with :ok <- Reader.write_command(state.cnc, message_type, command) do
      await_response(state, state.broadcast_position, correlation_id, deadline(state), [])
    end
  end

  defp await_response(state, position, correlation_id, deadline, image_fields) do
    receiver = Reader.to_clients(state.cnc)

    receiver
    |> Receiver.receive(position)
    |> handle_received(receiver, state, correlation_id, deadline, image_fields)
  end

  defp handle_received(
         {:error, :lapped} = drained,
         receiver,
         _state,
         _correlation_id,
         _deadline,
         _image_fields
       ),
       do: {:error, :broadcast_lapped, next_broadcast_position(drained, receiver)}

  defp handle_received(
         {:ok, records, next_position},
         _receiver,
         state,
         correlation_id,
         deadline,
         image_fields
       ) do
    seen_images = image_fields ++ collect_driver_events(records)

    records
    |> match_response(correlation_id)
    |> settle_response(state, next_position, correlation_id, deadline, seen_images)
  end

  defp settle_response({:ok, response}, _state, position, _correlation_id, _deadline, images),
    do: {:ok, response, position, images}

  defp settle_response(:no_match, state, position, correlation_id, deadline, images),
    do: continue_await(now_ms() >= deadline, state, position, correlation_id, deadline, images)

  defp continue_await(true, _state, _position, _correlation_id, _deadline, _image_fields),
    do: {:error, :driver_timeout}

  defp continue_await(false, state, position, correlation_id, deadline, image_fields) do
    Process.sleep(@poll_interval_ms)
    await_response(state, position, correlation_id, deadline, image_fields)
  end

  defp collect_driver_events(records) do
    for {type_id, payload} <- records,
        {:ok, {tag, fields}} <- [Control.decode(type_id, payload)],
        tag in [:on_available_image, :on_unavailable_image, :on_client_timeout] do
      {tag, fields}
    end
  end

  defp apply_driver_events(state, image_events) do
    Enum.reduce(image_events, state, &apply_driver_event/2)
  end

  defp apply_driver_event({:on_available_image, fields}, state) do
    %{state | images: add_image(state.images, fields, state)}
  end

  defp apply_driver_event(
         {:on_client_timeout, %{client_id: client_id}},
         %{client_id: client_id} = state
       ),
       do: %{state | client_timed_out?: true}

  defp apply_driver_event({:on_client_timeout, _fields}, state), do: state

  defp apply_driver_event({:on_unavailable_image, fields}, state) do
    state.images
    |> Map.pop({fields.subscription_registration_id, fields.correlation_id})
    |> drop_image(state)
  end

  defp drop_image({nil, _images}, state), do: state

  defp drop_image({image, images}, state) do
    close_handle(image)

    RuntimeHandles.delete_image(
      state.handles,
      image.subscription_registration_id,
      image.correlation_id
    )

    notify_image_handler(state, image.subscription_registration_id, :unavailable, image)
    %{state | images: images}
  end

  defp notify_image_handler(state, subscription_registration_id, event, image) do
    handler = get_in(state.subscriptions, [subscription_registration_id, event])

    summary = %{
      session_id: image.session_id,
      correlation_id: image.correlation_id,
      subscription_registration_id: subscription_registration_id,
      source_identity: image.source_identity
    }

    invoke_image_handler(handler, event, summary)
  end

  defp invoke_image_handler(handler, _event, _summary) when not is_function(handler, 1), do: :ok

  defp invoke_image_handler(handler, event, summary) do
    handler.(summary)
  rescue
    exception ->
      Logger.error(
        "#{event} image handler raised: #{Exception.format(:error, exception, __STACKTRACE__)}"
      )
  end

  defp match_response([], _correlation_id), do: :no_match

  defp match_response([{type_id, payload} | rest], correlation_id) do
    type_id
    |> Control.decode(payload)
    |> match_decoded(rest, correlation_id)
  end

  defp match_decoded({:ok, {tag, fields} = response}, rest, correlation_id) do
    select_match(
      matches_correlation?(tag, fields, correlation_id),
      response,
      rest,
      correlation_id
    )
  end

  defp match_decoded({:error, _reason}, rest, correlation_id),
    do: match_response(rest, correlation_id)

  defp select_match(true, response, _rest, _correlation_id), do: {:ok, response}

  defp select_match(false, _response, rest, correlation_id),
    do: match_response(rest, correlation_id)

  defp matches_correlation?(:on_error, fields, correlation_id),
    do: fields.offending_command_correlation_id == correlation_id

  defp matches_correlation?(_tag, %{correlation_id: id}, correlation_id), do: id == correlation_id
  defp matches_correlation?(_tag, _fields, _correlation_id), do: false

  defp register_publication(fields, registration_id, position, state) do
    fields.log_file_name
    |> map_log_descriptor(state.cnc)
    |> reply_mapped_publication(fields, registration_id, %{state | broadcast_position: position})
  end

  defp reply_mapped_publication({:error, reason}, _fields, _registration_id, state),
    do: {:reply, {:error, reason}, state}

  defp reply_mapped_publication({:ok, mapping, descriptor}, fields, registration_id, state) do
    publication =
      Publisher.new(
        registration_id,
        mapping,
        fields.session_id,
        fields.stream_id,
        descriptor,
        counter_address(state.cnc, fields.publication_limit_counter_id)
      )

    RuntimeHandles.put_publication(state.handles, publication)
    publications = Map.put(state.publications, registration_id, publication)

    {:reply, {:ok, public_publication_fields(fields, registration_id, descriptor, publication)},
     %{state | publications: publications}}
  end

  defp public_publication_fields(fields, registration_id, descriptor, publication) do
    %{
      handle: publication,
      registration_id: registration_id,
      session_id: fields.session_id,
      stream_id: fields.stream_id,
      initial_term_id: descriptor.initial_term_id,
      term_buffer_length: descriptor.term_buffer_length,
      max_message_length: descriptor.max_message_length,
      max_payload_length: descriptor.max_payload_length,
      position_bits_to_shift: descriptor.position_bits_to_shift,
      max_possible_position: descriptor.max_possible_position,
      publication_limit_counter_id: fields.publication_limit_counter_id,
      channel_status_indicator_id: fields.channel_status_indicator_id
    }
  end

  defp drain_available_images(state) do
    receiver = Reader.to_clients(state.cnc)

    receiver
    |> Receiver.receive(state.broadcast_position)
    |> merge_drained(state, receiver)
  end

  defp merge_drained({:ok, records, _next_position} = drained, state, receiver) do
    state
    |> apply_driver_events(collect_driver_events(records))
    |> Map.put(:broadcast_position, next_broadcast_position(drained, receiver))
  end

  defp merge_drained({:error, :lapped} = drained, state, receiver),
    do: %{state | broadcast_position: next_broadcast_position(drained, receiver)}

  defp add_image(images, fields, state) do
    key = {fields.subscription_registration_id, fields.correlation_id}
    add_image_when_new(Map.has_key?(images, key), images, key, fields, state)
  end

  defp add_image_when_new(true, images, _key, _fields, _state), do: images

  defp add_image_when_new(false, images, key, fields, state) do
    fields.log_file_name
    |> map_log_descriptor(state.cnc)
    |> map_image(images, key, fields, state)
  end

  defp map_image({:ok, mapping, descriptor}, images, key, fields, state) do
    image =
      Subscriber.new(
        fields.correlation_id,
        fields.subscription_registration_id,
        mapping,
        fields.session_id,
        descriptor,
        counter_address(state.cnc, fields.subscriber_position_id),
        fields.source_identity
      )

    RuntimeHandles.put_image(state.handles, image)
    notify_image_handler(state, fields.subscription_registration_id, :available, image)
    Map.put(images, key, image)
  end

  defp map_image({:error, _reason}, images, _key, _fields, _state), do: images

  defp map_log_descriptor(log_file_name, cnc) do
    with {:ok, region, base, length} <- NIF.map_log(log_file_name),
         :ok <- NIF.retain_parent_region(region, cnc.region),
         {:ok, metadata} <- read_log_metadata(base, length),
         {:ok, descriptor} <- Descriptor.read(metadata) do
      {:ok, Mapping.new(region, base, length), descriptor}
    end
  end

  defp counter_address(cnc, counter_id) do
    Reader.counter_address(cnc, counter_id)
  end

  defp read_log_metadata(base, length) do
    meta_length = Descriptor.log_meta_data_length()
    NIF.read_binary(base + (length - meta_length), meta_length, 0)
  end

  defp deadline(state), do: now_ms() + state.driver_timeout_ms

  defp now_ms, do: System.system_time(:millisecond)
end
