defmodule AeronElixir.UdpTest do
  use ExUnit.Case

  alias AeronElixir.Header

  @moduletag :system

  setup do
    {:ok, client} = start_supervised(AeronElixir)

    channel = "aeron:udp?endpoint=127.0.0.1:#{unique_port()}"
    stream_id = :erlang.unique_integer([:positive])

    {:ok, publication} = AeronElixir.add_publication(client, channel, stream_id)
    {:ok, subscription} = AeronElixir.add_subscription(client, channel, stream_id)

    assert :ok = AeronElixir.await_connected(publication, 5_000)
    assert :ok = AeronElixir.await_connected(subscription, 5_000)

    %{client: client, publication: publication, subscription: subscription}
  end

  describe "udp messaging" do
    test "poll_batch receives single-frame messages in order", %{
      publication: publication,
      subscription: subscription
    } do
      messages = for n <- 1..20, do: "udp-message-#{n}"

      for message <- messages do
        assert {:ok, _position} = AeronElixir.publish(publication, message)
      end

      assert collect_batch(subscription, 64, length(messages), []) == messages
    end

    test "poll_batch reassembles a multi-fragment message", %{
      publication: publication,
      subscription: subscription
    } do
      long = :binary.copy(<<0x5A>>, publication.max_payload_length * 3)
      messages = ["before", long, "after"]

      for message <- messages do
        assert {:ok, _position} = AeronElixir.publish(publication, message)
      end

      assert collect_batch(subscription, 64, length(messages), []) == messages
    end

    test "poll delivers payload and header to a handler", %{
      publication: publication,
      subscription: subscription
    } do
      assert {:ok, _position} = AeronElixir.publish(publication, "udp-handler")

      {:ok, agent} = Agent.start_link(fn -> [] end)
      handler = fn payload, header -> Agent.update(agent, &[{payload, header} | &1]) end

      assert 1 == poll_until(subscription, handler, 8)

      assert [{"udp-handler", header}] = Agent.get(agent, & &1)
      assert Header.stream_id(header) == publication.stream_id
      assert Header.session_id(header) == publication.session_id
      assert Header.frame(header).type == :data
    end
  end

  describe "udp term rotation" do
    setup ctx do
      channel = "aeron:udp?endpoint=127.0.0.1:#{unique_port() + 1}|term-length=65536"
      stream_id = :erlang.unique_integer([:positive])

      {:ok, publication} = AeronElixir.add_publication(ctx.client, channel, stream_id)
      {:ok, subscription} = AeronElixir.add_subscription(ctx.client, channel, stream_id)

      assert :ok = AeronElixir.await_connected(publication, 5_000)
      assert :ok = AeronElixir.await_connected(subscription, 5_000)

      %{publication: publication, subscription: subscription}
    end

    test "receives every message across multiple term rotations", %{
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

      publish_one = fn publish_one ->
        case AeronElixir.publish(publication, payload) do
          {:ok, position} ->
            position

          {:error, :back_pressured} ->
            drain.()
            publish_one.(publish_one)
        end
      end

      for _ <- 1..div(total, batch) do
        for _ <- 1..batch, do: publish_one.(publish_one)
        drain.()
      end

      drain_until(drain, received, total)

      assert Agent.get(received, & &1) == total
    end
  end

  defp drain_until(drain, received, total, attempts \\ 400)
  defp drain_until(_drain, _received, _total, 0), do: :ok

  defp drain_until(drain, received, total, attempts) do
    drain.()

    case Agent.get(received, & &1) >= total do
      true ->
        :ok

      false ->
        Process.sleep(5)
        drain_until(drain, received, total, attempts - 1)
    end
  end

  defp unique_port, do: 40_000 + :erlang.phash2({System.pid(), System.system_time()}, 20_000)

  defp poll_until(subscription, handler, limit, attempts \\ 200)
  defp poll_until(_subscription, _handler, _limit, 0), do: 0

  defp poll_until(subscription, handler, limit, attempts) do
    case AeronElixir.poll(subscription, limit, handler) do
      0 ->
        Process.sleep(5)
        poll_until(subscription, handler, limit, attempts - 1)

      count ->
        count
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

  describe "multi-destination cast" do
    test "a manual-control publication delivers to every added destination", %{client: client} do
      stream_id = :erlang.unique_integer([:positive])
      first_port = 40_000 + :rand.uniform(10_000)
      second_port = first_port + 1

      {:ok, publication} =
        AeronElixir.add_publication(client, "aeron:udp?control-mode=manual", stream_id)

      first = "aeron:udp?endpoint=127.0.0.1:#{first_port}"
      second = "aeron:udp?endpoint=127.0.0.1:#{second_port}"

      {:ok, first_sub} = AeronElixir.add_subscription(client, first, stream_id)
      {:ok, second_sub} = AeronElixir.add_subscription(client, second, stream_id)

      assert :ok = AeronElixir.add_destination(publication, first)
      assert :ok = AeronElixir.add_destination(publication, second)

      assert :ok = AeronElixir.await_connected(first_sub, 5_000)
      assert :ok = AeronElixir.await_connected(second_sub, 5_000)

      {:ok, _} = AeronElixir.publish(publication, "to both destinations")

      assert ["to both destinations"] = drain(first_sub, 1)
      assert ["to both destinations"] = drain(second_sub, 1)
    end

    test "a removed destination stops receiving", %{client: client} do
      stream_id = :erlang.unique_integer([:positive])
      port = 40_000 + :rand.uniform(10_000)
      destination = "aeron:udp?endpoint=127.0.0.1:#{port}"

      {:ok, publication} =
        AeronElixir.add_publication(client, "aeron:udp?control-mode=manual", stream_id)

      {:ok, subscription} = AeronElixir.add_subscription(client, destination, stream_id)

      :ok = AeronElixir.add_destination(publication, destination)
      :ok = AeronElixir.await_connected(publication, 5_000)
      {:ok, _} = AeronElixir.publish(publication, "before removal")
      assert ["before removal"] = drain(subscription, 1)

      assert :ok = AeronElixir.remove_destination(publication, destination)
      Process.sleep(200)
      _ = AeronElixir.try_publish(publication, "after removal")

      assert [] == drain(subscription, 1, 20)
    end

    test "a manual-control subscription receives from every added destination", %{client: client} do
      stream_id = :erlang.unique_integer([:positive])
      first_port = 41_000 + :rand.uniform(10_000)
      second_port = first_port + 1
      first = "aeron:udp?endpoint=127.0.0.1:#{first_port}"
      second = "aeron:udp?endpoint=127.0.0.1:#{second_port}"

      {:ok, subscription} =
        AeronElixir.add_subscription(client, "aeron:udp?control-mode=manual", stream_id)

      assert :ok = AeronElixir.add_destination(subscription, first)
      assert :ok = AeronElixir.add_destination(subscription, second)

      {:ok, first_pub} = AeronElixir.add_publication(client, first, stream_id)
      {:ok, second_pub} = AeronElixir.add_publication(client, second, stream_id)

      :ok = AeronElixir.await_connected(first_pub, 5_000)
      :ok = AeronElixir.await_connected(second_pub, 5_000)

      {:ok, _} = AeronElixir.publish(first_pub, "from first")
      {:ok, _} = AeronElixir.publish(second_pub, "from second")

      received = drain(subscription, 2)
      assert "from first" in received
      assert "from second" in received
    end
  end

  defp drain(subscription, target, attempts \\ 200)
  defp drain(_subscription, _target, 0), do: []

  defp drain(subscription, target, attempts) do
    {:ok, _count, payloads} = AeronElixir.poll_batch(subscription, 64)

    case payloads do
      [] ->
        Process.sleep(5)
        drain(subscription, target, attempts - 1)

      received when length(received) >= target ->
        received

      partial ->
        Process.sleep(5)
        partial ++ drain(subscription, target - length(partial), attempts - 1)
    end
  end
end
