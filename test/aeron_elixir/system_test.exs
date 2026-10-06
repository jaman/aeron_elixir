defmodule AeronElixir.SystemTest do
  use ExUnit.Case

  alias AeronElixir.Header

  @moduletag :system

  describe "client lifecycle" do
    test "can create a client" do
      {:ok, _client} = start_supervised(AeronElixir)
      assert true
    end

    test "stopping the client process closes its client" do
      {:ok, owner} = start_supervised(AeronElixir)
      client = AeronElixir.client(owner)
      conductor = monitor_conductor(client)

      :ok = stop_supervised(AeronElixir)

      assert_receive {:DOWN, ^conductor, :process, _pid, :shutdown}, 5_000
      assert %{status: :closed} = Ash.get!(AeronElixir.Resources.Client, client.id)
    end

    test "killing the client process stops its client" do
      {:ok, owner} = AeronElixir.start_link(name: nil)
      Process.unlink(owner)
      conductor = owner |> AeronElixir.client() |> monitor_conductor()

      Process.exit(owner, :kill)

      assert_receive {:DOWN, ^conductor, :process, _pid, :shutdown}, 5_000
    end
  end

  defp monitor_conductor(client) do
    [{conductor, _value}] = Registry.lookup(AeronElixir.Registry, {:client_conductor, client.id})
    Process.monitor(conductor)
  end

  describe "publication lifecycle" do
    setup do
      {:ok, client} = start_supervised(AeronElixir)
      %{client: client}
    end

    test "can create a publication", %{client: client} do
      channel = "aeron:ipc?alias=test-pub"
      stream_id = 1

      {:ok, publication} = AeronElixir.add_publication(client, channel, stream_id)
      assert is_struct(publication)
    end
  end

  describe "subscription lifecycle" do
    setup do
      {:ok, client} = start_supervised(AeronElixir)
      %{client: client}
    end

    test "can create a subscription", %{client: client} do
      channel = "aeron:ipc?alias=test-sub"
      stream_id = 1

      {:ok, subscription} = AeronElixir.add_subscription(client, channel, stream_id)
      assert is_struct(subscription)
    end
  end

  describe "basic message flow" do
    setup do
      {:ok, client} = start_supervised(AeronElixir)

      channel = "aeron:ipc?alias=test-flow-#{:erlang.unique_integer([:positive])}"
      stream_id = :erlang.unique_integer([:positive])

      {:ok, publication} = AeronElixir.add_publication(client, channel, stream_id)

      {:ok, subscription} = AeronElixir.add_subscription(client, channel, stream_id)

      %{client: client, publication: publication, subscription: subscription}
    end

    test "can publish a message", %{publication: publication} do
      message = "test message"

      {:ok, _} = AeronElixir.publish(publication, message)
    end

    test "can poll for messages", %{subscription: subscription} do
      message_count =
        AeronElixir.poll(subscription, 10, fn payload, _header ->
          IO.puts("Received: #{inspect(payload)}")
          :continue
        end)

      assert is_integer(message_count)
    end

    test "publishes a message and polls it back over the term buffer", %{
      publication: publication,
      subscription: subscription
    } do
      message = "round trip #{:erlang.unique_integer([:positive])}"
      assert message in round_trip(publication, subscription, message)
    end

    test "publishes a fragmented message and reassembles it on poll", %{
      publication: publication,
      subscription: subscription
    } do
      message = :binary.copy(<<0x5A>>, 4096)
      assert message in round_trip(publication, subscription, message)
    end

    test "round-trips an arbitrary binary payload byte-for-byte", %{
      publication: publication,
      subscription: subscription
    } do
      message = <<0, 1, 2, 255, 254, 0x7F, 0x80, "ünïcödé"::utf8, 0, 0>>
      received = round_trip(publication, subscription, message)

      assert message in received
      [matched] = Enum.filter(received, &(&1 == message))
      assert matched == message
      assert byte_size(matched) == byte_size(message)
    end
  end

  describe "sustained flow across term rotations" do
    setup do
      {:ok, client} = start_supervised(AeronElixir)

      channel =
        "aeron:ipc?alias=rotate-#{:erlang.unique_integer([:positive])}|term-length=65536"

      stream_id = :erlang.unique_integer([:positive])

      {:ok, publication} = AeronElixir.add_publication(client, channel, stream_id)
      {:ok, subscription} = AeronElixir.add_subscription(client, channel, stream_id)

      %{publication: publication, subscription: subscription}
    end

    test "receives every message across multiple term rotations and partition reuse", %{
      publication: publication,
      subscription: subscription
    } do
      payload = :binary.copy(<<0x5A>>, 32)
      total = 6_000
      batch = 200

      {:ok, received} = Agent.start_link(fn -> 0 end)
      handler = fn _payload, _header -> Agent.update(received, &(&1 + 1)) end

      drain = fn ->
        Stream.repeatedly(fn -> AeronElixir.poll(subscription, 64, handler) end)
        |> Enum.take_while(&(&1 > 0))
        |> Enum.sum()
      end

      for _ <- 1..div(total, batch) do
        for _ <- 1..batch, do: {:ok, _} = AeronElixir.publish(publication, payload)
        drain.()
      end

      eventually_drained(drain, received, total)

      assert Agent.get(received, & &1) == total
    end
  end

  describe "batch publish and consume" do
    setup do
      {:ok, client} = start_supervised(AeronElixir)

      channel = "aeron:ipc?alias=batch-#{:erlang.unique_integer([:positive])}"
      stream_id = :erlang.unique_integer([:positive])

      {:ok, publication} = AeronElixir.add_publication(client, channel, stream_id)
      {:ok, subscription} = AeronElixir.add_subscription(client, channel, stream_id)

      %{publication: publication, subscription: subscription}
    end

    test "publish_n appends a batch the subscriber receives in full", %{
      publication: publication,
      subscription: subscription
    } do
      payload = :binary.copy(<<0x5A>>, 32)

      assert {:ok, 500} = AeronElixir.publish_n(publication, payload, 500)

      {:ok, received} = Agent.start_link(fn -> 0 end)
      handler = fn _payload, _header -> Agent.update(received, &(&1 + 1)) end

      drain = fn ->
        Stream.repeatedly(fn -> AeronElixir.poll(subscription, 64, handler) end)
        |> Enum.take_while(&(&1 > 0))
        |> Enum.sum()
      end

      eventually_drained(drain, received, 500)

      assert Agent.get(received, & &1) == 500
    end

    test "poll_batch returns the received message payloads", %{
      publication: publication,
      subscription: subscription
    } do
      messages = for n <- 1..5, do: "batch-message-#{n}"
      for message <- messages, do: {:ok, _} = AeronElixir.publish(publication, message)

      payloads = collect_batch(subscription, 64, length(messages), [])

      for message <- messages do
        assert message in payloads
      end

      assert length(payloads) == length(messages)
    end

    test "direct handles publish and consume without routing through the conductor", %{
      publication: publication,
      subscription: subscription
    } do
      payload = :binary.copy(<<0x5A>>, 64)
      {:ok, handle} = AeronElixir.publication_handle(publication)
      {:ok, [image]} = AeronElixir.subscription_handles(subscription)

      # publish from a separate process via the handle (no GenServer hop)
      task = Task.async(fn -> AeronElixir.publish_n(handle, payload, 300) end)
      assert {:ok, 300} = Task.await(task)

      payloads = collect_batch(image, 64, 300, [])
      assert length(payloads) == 300
      assert Enum.all?(payloads, &(&1 == payload))
    end

    test "poll_batch reassembles a multi-fragment message", %{
      publication: publication,
      subscription: subscription
    } do
      message = :binary.copy(<<0x5A>>, 8192)
      {:ok, _} = AeronElixir.publish(publication, message)

      [payload] = collect_batch(subscription, 64, 1, [])

      assert payload == message
      assert byte_size(payload) == 8192
    end

    test "poll_image_next returns one whole message per call, then :empty", %{
      publication: publication,
      subscription: subscription
    } do
      messages = ["small", :binary.copy(<<0x5A>>, 8192), :binary.copy(<<0x3C>>, 200)]
      {:ok, [image]} = AeronElixir.subscription_handles(subscription)
      for message <- messages, do: {:ok, _} = AeronElixir.publish(publication, message)

      assert next_messages(image, length(messages), []) == messages
      assert AeronElixir.poll_image_next(image) == :empty
    end
  end

  describe "hot path independence from the conductor" do
    setup do
      {:ok, client} = start_supervised(AeronElixir)

      channel = "aeron:ipc?alias=hot-#{:erlang.unique_integer([:positive])}"
      stream_id = :erlang.unique_integer([:positive])

      {:ok, publication} = AeronElixir.add_publication(client, channel, stream_id)
      {:ok, subscription} = AeronElixir.add_subscription(client, channel, stream_id)

      [{conductor, _}] =
        Registry.lookup(AeronElixir.Registry, {:client_conductor, publication.client_id})

      %{publication: publication, subscription: subscription, conductor: conductor}
    end

    test "publish and poll_batch complete while the conductor is suspended", %{
      publication: publication,
      subscription: subscription,
      conductor: conductor
    } do
      {:ok, _} = AeronElixir.publish(publication, "prime")
      [_] = collect_batch(subscription, 64, 1, [])

      :ok = :sys.suspend(conductor)

      try do
        assert {:ok, _position} = AeronElixir.publish(publication, "while-suspended")
        assert {:ok, 1, ["while-suspended"]} = AeronElixir.poll_batch(subscription, 64)

        {:ok, _position} = AeronElixir.publish(publication, "framed")
        {:ok, agent} = Agent.start_link(fn -> [] end)
        handler = fn payload, header -> Agent.update(agent, &[{payload, header} | &1]) end
        assert 1 == AeronElixir.poll(subscription, 8, handler)

        assert [{"framed", header}] = Agent.get(agent, & &1)
        assert Header.stream_id(header) == publication.stream_id
        assert Header.session_id(header) == publication.session_id
        assert Header.frame(header).type == :data
      after
        :sys.resume(conductor)
      end
    end

    test "eight concurrent writers on one publication deliver every message intact", %{
      publication: publication,
      subscription: subscription
    } do
      writers = 8
      per_writer = 500
      {:ok, handle} = AeronElixir.publication_handle(publication)
      {:ok, [image]} = AeronElixir.subscription_handles(subscription)

      tasks =
        for writer <- 1..writers do
          Task.async(fn ->
            payload = <<writer>> <> :binary.copy(<<writer>>, 95)
            publish_repeatedly(handle, payload, per_writer)
          end)
        end

      payloads = collect_batch(image, 256, writers * per_writer, [])
      Task.await_many(tasks, 30_000)

      assert length(payloads) == writers * per_writer

      counts = Enum.frequencies_by(payloads, &binary_part(&1, 0, 1))

      for writer <- 1..writers do
        assert counts[<<writer>>] == per_writer
      end

      assert Enum.all?(payloads, fn <<writer, rest::binary>> ->
               rest == :binary.copy(<<writer>>, 95)
             end)
    end
  end

  describe "list publishing" do
    setup do
      {:ok, client} = start_supervised(AeronElixir)

      channel = "aeron:ipc?alias=list-#{:erlang.unique_integer([:positive])}"
      stream_id = :erlang.unique_integer([:positive])

      {:ok, publication} = AeronElixir.add_publication(client, channel, stream_id)
      {:ok, subscription} = AeronElixir.add_subscription(client, channel, stream_id)

      %{publication: publication, subscription: subscription}
    end

    test "publish_list appends distinct payloads in order in one call", %{
      publication: publication,
      subscription: subscription
    } do
      messages = for n <- 1..300, do: <<n::32>> <> :binary.copy(<<n>>, rem(n, 200))

      assert {:ok, 300} = AeronElixir.publish_list(publication, messages)
      assert collect_batch(subscription, 512, 300, []) == messages
    end

    test "publish_list through a direct handle works from another process", %{
      publication: publication,
      subscription: subscription
    } do
      {:ok, handle} = AeronElixir.publication_handle(publication)
      messages = for n <- 1..50, do: "handle-#{n}"

      assert {:ok, 50} =
               Task.await(Task.async(fn -> AeronElixir.publish_list(handle, messages) end))

      assert collect_batch(subscription, 64, 50, []) == messages
    end

    test "publish and publish_list accept iodata and write it contiguously", %{
      publication: publication,
      subscription: subscription
    } do
      tail = :binary.copy(<<0xAB>>, 200)
      single = [<<1::32>>, [<<2::64>>, ?x], tail]
      listed = for n <- 1..100, do: [<<n::32>>, tail]

      assert {:ok, _} = AeronElixir.publish(publication, single)
      assert {:ok, 100} = AeronElixir.publish_list(publication, listed)

      [first | rest] = collect_batch(subscription, 256, 101, [])
      assert first == IO.iodata_to_binary(single)
      assert rest == Enum.map(listed, &IO.iodata_to_binary/1)
    end

    test "iodata of more than sixteen pieces is written whole", %{
      publication: publication,
      subscription: subscription
    } do
      pieces = for n <- 1..40, do: if(rem(n, 3) == 0, do: n, else: <<n::16, 0xCD>>)
      many = [pieces | "improper tail"]

      assert {:ok, _} = AeronElixir.publish(publication, many)
      assert {:ok, 2} = AeronElixir.publish_list(publication, [many, pieces])

      assert collect_batch(subscription, 8, 3, []) ==
               [IO.iodata_to_binary(many), IO.iodata_to_binary(many), IO.iodata_to_binary(pieces)]
    end

    test "BatchPublisher delivers every message sent to it, in order", %{
      publication: publication,
      subscription: subscription
    } do
      {:ok, batcher} = start_supervised({AeronElixir.BatchPublisher, publication: publication})
      messages = for n <- 1..2000, do: <<n::32, 0::224>>

      collector = Task.async(fn -> collect_batch(subscription, 1024, 2000, [], 4000) end)

      for message <- messages, do: :ok = AeronElixir.BatchPublisher.publish(batcher, message)
      :ok = AeronElixir.BatchPublisher.flush(batcher, 30_000)

      assert Task.await(collector, 30_000) == messages
      assert AeronElixir.BatchPublisher.pending(batcher) == 0
    end
  end

  describe "counter lifecycle" do
    setup do
      {:ok, client} = start_supervised(AeronElixir)
      %{client: client}
    end

    test "adds, increments and reads back a counter against the live driver", %{client: client} do
      type_id = 1_000
      label = "elixir-counter-#{:erlang.unique_integer([:positive])}"

      {:ok, counter} = AeronElixir.add_counter(client, type_id, <<>>, label)

      assert counter.counter_id >= 0
      assert counter.value_address > 0
      assert {:ok, 0} = AeronElixir.counter_value(counter)

      deltas = [7, 35, 100, 5]
      expected = Enum.sum(deltas)

      final =
        Enum.reduce(deltas, counter, fn delta, acc ->
          {:ok, updated} = AeronElixir.increment_counter(acc, delta)
          updated
        end)

      assert final.value == expected
      assert {:ok, ^expected} = AeronElixir.counter_value(final)

      {:ok, closed} = AeronElixir.close(final)
      assert closed.is_closed
    end
  end

  defp publish_repeatedly(_handle, _payload, 0), do: :ok

  defp publish_repeatedly(handle, payload, remaining) do
    case AeronElixir.publish(handle, payload) do
      {:ok, _position} -> publish_repeatedly(handle, payload, remaining - 1)
      {:error, _back_pressured} -> publish_repeatedly(handle, payload, remaining)
    end
  end

  defp collect_batch(subscription, limit, target, acc, attempts \\ 200)
  defp collect_batch(_subscription, _limit, _target, acc, 0), do: acc

  defp collect_batch(subscription, limit, target, acc, attempts) do
    {:ok, _count, payloads} = AeronElixir.poll_batch(subscription, limit)
    acc = acc ++ payloads

    if length(acc) >= target do
      acc
    else
      Process.sleep(5)
      collect_batch(subscription, limit, target, acc, attempts - 1)
    end
  end

  defp next_messages(image, target, acc, attempts \\ 200)

  defp next_messages(_image, target, acc, _attempts) when length(acc) == target,
    do: Enum.reverse(acc)

  defp next_messages(_image, _target, acc, 0), do: Enum.reverse(acc)

  defp next_messages(image, target, acc, attempts) do
    case AeronElixir.poll_image_next(image) do
      {:ok, payload} ->
        next_messages(image, target, [payload | acc], attempts)

      :empty ->
        Process.sleep(5)
        next_messages(image, target, acc, attempts - 1)
    end
  end

  defp eventually_drained(drain, received, total, attempts \\ 200)
  defp eventually_drained(_drain, _received, _total, 0), do: :ok

  defp eventually_drained(drain, received, total, attempts) do
    drain.()

    if Agent.get(received, & &1) >= total do
      :ok
    else
      Process.sleep(5)
      eventually_drained(drain, received, total, attempts - 1)
    end
  end

  defp round_trip(publication, subscription, message) do
    {:ok, agent} = Agent.start_link(fn -> [] end)

    handler = fn payload, _header ->
      Agent.update(agent, fn acc -> [payload | acc] end)
      :continue
    end

    await_round_trip(publication, subscription, message, agent, handler)
  end

  defp await_round_trip(publication, subscription, message, agent, handler, attempts \\ 100)

  defp await_round_trip(_publication, _subscription, _message, agent, _handler, 0),
    do: Agent.get(agent, & &1)

  defp await_round_trip(publication, subscription, message, agent, handler, attempts) do
    AeronElixir.publish(publication, message)
    AeronElixir.poll(subscription, 10, handler)
    received = Agent.get(agent, & &1)

    if message in received do
      received
    else
      Process.sleep(20)
      await_round_trip(publication, subscription, message, agent, handler, attempts - 1)
    end
  end
end
