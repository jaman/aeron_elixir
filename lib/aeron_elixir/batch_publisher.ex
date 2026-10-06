defmodule AeronElixir.BatchPublisher do
  @moduledoc """
  A process that publishes on one publication, sending everything queued in its
  mailbox as a single `AeronElixir.publish_list/2` call each time it runs.

  `publish/2` is asynchronous and never blocks the caller. Each time the process
  wakes it drains every queued message and appends them all in one native call,
  so under load the cost of the NIF boundary is shared across the whole burst,
  while a lone message is appended as soon as it arrives. Messages are appended
  in the order they were sent by a given caller.

  Messages the publication back-pressures stay queued and are retried every
  `retry_interval_ms` (default 1) until the subscriber drains; nothing is
  dropped. `flush/1` blocks until the queue is empty. `pending/1` reports the
  number of messages still queued.

  When the publication is closed, by `AeronElixir.close/1` or because the client
  lost its media driver, the process replies `{:error, :closed}` to every waiting
  `flush/1` and stops with `{:shutdown, :closed}`; queued messages are not sent.
  Place it after the client in a `:rest_for_one` supervisor so it starts again
  with a new publication.

  Start with `{AeronElixir.BatchPublisher, publication: publication}` where
  `publication` is an `AeronElixir.Resources.Publication` record or a
  `AeronElixir.LogBuffer.Publisher` handle; the handle is resolved once at start.
  """

  use GenServer

  alias AeronElixir.LogBuffer.Publisher
  alias AeronElixir.Resources.Publication
  alias AeronElixir.RuntimeHandles

  @default_retry_interval_ms 1

  defstruct [:handle, :retry_interval_ms, :retry_timer, queue: [], waiting: []]

  @type option ::
          {:publication, Publication.t() | Publisher.t()}
          | {:retry_interval_ms, pos_integer()}
          | {:name, GenServer.name()}

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    {name_opts, init_opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, init_opts, name_opts)
  end

  @doc """
  Queues `payload` for the next batch; never blocks.
  """
  @spec publish(GenServer.server(), iodata()) :: :ok
  def publish(server, payload) when is_binary(payload) or is_list(payload) do
    send(server, {:publish, payload})
    :ok
  end

  @doc """
  Blocks until every queued message has been appended to the publication.

  A back-pressured publication is retried until the subscriber drains, so a
  caller that fills the publication without anything consuming it waits for the
  whole `timeout`.
  """
  @spec flush(GenServer.server(), timeout()) :: :ok | {:error, :closed}
  def flush(server, timeout \\ 5_000) do
    GenServer.call(server, :flush, timeout)
  end

  @doc """
  Returns the number of queued messages not yet appended.
  """
  @spec pending(GenServer.server()) :: non_neg_integer()
  def pending(server) do
    GenServer.call(server, :pending)
  end

  @impl true
  def init(opts) do
    retry_interval_ms = Keyword.get(opts, :retry_interval_ms, @default_retry_interval_ms)

    case resolve_handle(Keyword.fetch!(opts, :publication)) do
      {:ok, handle} -> {:ok, %__MODULE__{handle: handle, retry_interval_ms: retry_interval_ms}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_info({:publish, payload}, state) do
    batch = drain_mailbox([payload])
    append(%{state | queue: state.queue ++ batch})
  end

  def handle_info(:retry, state) do
    append(%{state | retry_timer: nil})
  end

  @impl true
  def handle_call(:flush, _from, %{queue: []} = state) do
    {:reply, :ok, state}
  end

  def handle_call(:flush, from, state) do
    append(%{state | waiting: [from | state.waiting]})
  end

  def handle_call(:pending, _from, state) do
    {:reply, length(state.queue), state}
  end

  defp resolve_handle(%Publisher{} = handle), do: {:ok, handle}

  defp resolve_handle(%Publication{client_id: client_id, registration_id: registration_id}) do
    RuntimeHandles.fetch_publication(client_id, registration_id)
  end

  defp drain_mailbox(acc) do
    receive do
      {:publish, payload} -> drain_mailbox([payload | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp append(%{queue: []} = state), do: {:noreply, reply_waiting(state, :ok)}

  defp append(state) do
    state.handle
    |> Publisher.publish_list(state.queue, 0)
    |> appended(state)
  end

  defp appended({:error, :closed}, state),
    do: {:stop, {:shutdown, :closed}, reply_waiting(state, {:error, :closed})}

  defp appended(published, state),
    do: {:noreply, settle_queue(%{state | queue: Enum.drop(state.queue, published)})}

  defp settle_queue(%{queue: []} = state), do: reply_waiting(state, :ok)
  defp settle_queue(state), do: schedule_retry(state)

  defp schedule_retry(%{retry_timer: nil} = state) do
    %{state | retry_timer: Process.send_after(self(), :retry, state.retry_interval_ms)}
  end

  defp schedule_retry(state), do: state

  defp reply_waiting(%{waiting: []} = state, _reply), do: state

  defp reply_waiting(state, reply) do
    Enum.each(state.waiting, &GenServer.reply(&1, reply))
    %{state | waiting: []}
  end
end
