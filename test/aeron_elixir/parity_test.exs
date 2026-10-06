defmodule AeronElixir.ParityTest do
  use ExUnit.Case

  alias AeronElixir.Header
  alias AeronElixir.LogBuffer.Subscriber

  @moduletag :system

  setup do
    {:ok, client} = start_supervised(AeronElixir)
    channel = "aeron:ipc?alias=parity-#{:erlang.unique_integer([:positive])}"
    stream_id = :erlang.unique_integer([:positive])
    %{client: client, channel: channel, stream_id: stream_id}
  end

  describe "offer status" do
    test "try_publish reports not_connected before any subscriber exists", ctx do
      {:ok, publication} = AeronElixir.add_publication(ctx.client, ctx.channel, ctx.stream_id)

      refute AeronElixir.connected?(publication)
      assert {:error, :not_connected} = AeronElixir.try_publish(publication, "nobody home")
    end

    test "try_publish succeeds once connected and reports back_pressure when the window fills",
         ctx do
      channel = ctx.channel <> "|term-length=65536"
      {:ok, publication} = AeronElixir.add_publication(ctx.client, channel, ctx.stream_id)
      {:ok, subscription} = AeronElixir.add_subscription(ctx.client, channel, ctx.stream_id)

      assert :ok = AeronElixir.await_connected(publication, 5_000)
      assert :ok = AeronElixir.await_connected(subscription, 5_000)
      assert AeronElixir.connected?(publication)
      assert AeronElixir.connected?(subscription)

      payload = :binary.copy(<<1>>, 1024)

      assert {:ok, _position} =
               Stream.repeatedly(fn -> AeronElixir.try_publish(publication, payload) end)
               |> Stream.take(5_000)
               |> Enum.find({:error, :window_never_opened}, &match?({:ok, _}, &1))

      results =
        Stream.repeatedly(fn -> AeronElixir.try_publish(publication, payload) end)
        |> Enum.take(200)

      assert Enum.any?(results, &(&1 == {:error, :back_pressured}))
      assert AeronElixir.position(publication) > 0
      assert AeronElixir.position_limit(publication) > 0
    end

    test "publishing on a closed publication returns closed", ctx do
      {:ok, publication} = AeronElixir.add_publication(ctx.client, ctx.channel, ctx.stream_id)
      {:ok, closed} = AeronElixir.close(publication)

      assert AeronElixir.closed?(closed)
      assert {:error, :closed} = AeronElixir.try_publish(publication, "late")
      assert {:error, :closed} = AeronElixir.publish(publication, "late")
    end
  end

  describe "exclusive publication" do
    test "add_exclusive_publication round-trips like a publication", ctx do
      {:ok, publication} =
        AeronElixir.add_exclusive_publication(ctx.client, ctx.channel, ctx.stream_id)

      assert publication.is_exclusive
      {:ok, subscription} = AeronElixir.add_subscription(ctx.client, ctx.channel, ctx.stream_id)
      assert :ok = AeronElixir.await_connected(publication, 5_000)
      assert {:ok, _} = AeronElixir.publish(publication, "exclusive")
      assert ["exclusive"] = collect(subscription, 1)
    end

    test "two exclusive publications on one stream get distinct sessions", ctx do
      {:ok, first} = AeronElixir.add_exclusive_publication(ctx.client, ctx.channel, ctx.stream_id)

      {:ok, second} =
        AeronElixir.add_exclusive_publication(ctx.client, ctx.channel, ctx.stream_id)

      assert first.session_id != second.session_id
    end
  end

  describe "images" do
    test "a subscription polled before a publisher joins delivers that publisher's messages",
         ctx do
      {:ok, subscription} = AeronElixir.add_subscription(ctx.client, ctx.channel, ctx.stream_id)
      {:ok, first} = AeronElixir.add_exclusive_publication(ctx.client, ctx.channel, ctx.stream_id)
      :ok = AeronElixir.await_connected(first, 5_000)
      wait_until(fn -> AeronElixir.image_count(subscription) == 1 end)
      assert {:ok, 0, []} = AeronElixir.poll_batch(subscription, 8)

      {:ok, second} =
        AeronElixir.add_exclusive_publication(ctx.client, ctx.channel, ctx.stream_id)

      :ok = AeronElixir.await_connected(second, 5_000)
      wait_until(fn -> AeronElixir.image_count(subscription) == 2 end)
      {:ok, _} = AeronElixir.publish(second, "from-second")

      wait_until(fn -> AeronElixir.poll_batch(subscription, 8) == {:ok, 1, ["from-second"]} end)
    end

    test "polling a subscription whose client has stopped reports the client closed", ctx do
      {:ok, subscription} = AeronElixir.add_subscription(ctx.client, ctx.channel, ctx.stream_id)
      assert {:ok, 0, []} = AeronElixir.poll_batch(subscription, 8)

      {:ok, _closed} = ctx.client |> AeronElixir.client() |> AeronElixir.close()

      wait_until(fn -> AeronElixir.poll_batch(subscription, 8) == {:error, :closed} end)
    end

    test "one subscription exposes one image per publication session", ctx do
      {:ok, subscription} = AeronElixir.add_subscription(ctx.client, ctx.channel, ctx.stream_id)
      {:ok, first} = AeronElixir.add_exclusive_publication(ctx.client, ctx.channel, ctx.stream_id)

      {:ok, second} =
        AeronElixir.add_exclusive_publication(ctx.client, ctx.channel, ctx.stream_id)

      :ok = AeronElixir.await_connected(first, 5_000)
      :ok = AeronElixir.await_connected(second, 5_000)
      wait_until(fn -> AeronElixir.image_count(subscription) == 2 end)

      {:ok, _} = AeronElixir.publish(first, "from-first")
      {:ok, _} = AeronElixir.publish(second, "from-second")

      {:ok, image} = AeronElixir.image_by_session_id(subscription, second.session_id)
      assert %Subscriber{session_id: session_id} = image
      assert session_id == second.session_id
      assert AeronElixir.image_position(image) == 0
      assert is_binary(image.source_identity)

      wait_until(fn ->
        match?({:ok, 1, ["from-second"]}, AeronElixir.poll_image_batch(image, 8))
      end)

      assert AeronElixir.image_position(image) > 0
      refute AeronElixir.end_of_stream?(image)

      {:ok, first_image} = AeronElixir.image_by_session_id(subscription, first.session_id)
      {:ok, agent} = Agent.start_link(fn -> [] end)

      wait_until(fn ->
        AeronElixir.poll_image(first_image, 8, fn payload, header ->
          Agent.update(agent, &[{payload, header} | &1])
        end) ==
          1
      end)

      assert [{"from-first", header}] = Agent.get(agent, & &1)
      assert Header.position(header) == Subscriber.position(first_image)
      assert Header.initial_term_id(header) == first_image.initial_term_id
      assert {:error, :unknown_session} = AeronElixir.image_by_session_id(subscription, -1)
    end

    test "available and unavailable image callbacks fire", ctx do
      test_pid = self()

      {:ok, subscription} =
        AeronElixir.add_subscription(ctx.client, ctx.channel, ctx.stream_id,
          available_image: fn image -> send(test_pid, {:available, image}) end,
          unavailable_image: fn image -> send(test_pid, {:unavailable, image}) end
        )

      {:ok, publication} = AeronElixir.add_publication(ctx.client, ctx.channel, ctx.stream_id)
      :ok = AeronElixir.await_connected(publication, 5_000)

      assert_receive {:available, %{session_id: session_id, subscription_registration_id: reg}},
                     5_000

      assert session_id == publication.session_id
      assert reg == subscription.registration_id

      {:ok, _} = AeronElixir.close(publication)
      assert_receive {:unavailable, %{session_id: ^session_id}}, 15_000
      wait_until(fn -> AeronElixir.image_count(subscription) == 0 end, 15_000)
    end
  end

  describe "controlled poll" do
    test "abort leaves the message for the next poll, break stops after it", ctx do
      {:ok, publication} = AeronElixir.add_publication(ctx.client, ctx.channel, ctx.stream_id)
      {:ok, subscription} = AeronElixir.add_subscription(ctx.client, ctx.channel, ctx.stream_id)
      :ok = AeronElixir.await_connected(publication, 5_000)
      for n <- 1..4, do: {:ok, _} = AeronElixir.publish(publication, "m#{n}")
      wait_until(fn -> AeronElixir.image_count(subscription) == 1 end)
      seen = fn -> Agent.start_link(fn -> [] end) end
      {:ok, agent} = seen.()

      aborting = fn buffer, _header ->
        Agent.update(agent, &[buffer | &1])
        if buffer == "m2", do: :abort, else: :continue
      end

      assert AeronElixir.controlled_poll(subscription, 10, aborting) == 1
      assert Agent.get(agent, &Enum.reverse/1) == ["m1", "m2"]

      {:ok, agent} = seen.()

      breaking = fn buffer, _header ->
        Agent.update(agent, &[buffer | &1])
        if buffer == "m3", do: :break, else: :continue
      end

      assert AeronElixir.controlled_poll(subscription, 10, breaking) == 2
      assert Agent.get(agent, &Enum.reverse/1) == ["m2", "m3"]

      {:ok, agent} = seen.()
      committing = fn buffer, _header -> Agent.update(agent, &[buffer | &1]) && :commit end
      assert AeronElixir.controlled_poll(subscription, 10, committing) == 1
      assert Agent.get(agent, &Enum.reverse/1) == ["m4"]
      assert AeronElixir.controlled_poll(subscription, 10, committing) == 0
    end
  end

  defp collect(subscription, target, acc \\ [], attempts \\ 500)
  defp collect(_subscription, _target, acc, 0), do: acc

  defp collect(subscription, target, acc, attempts) do
    {:ok, _count, payloads} = AeronElixir.poll_batch(subscription, 64)
    acc = acc ++ payloads

    if length(acc) >= target do
      acc
    else
      Process.sleep(2)
      collect(subscription, target, acc, attempts - 1)
    end
  end

  defp wait_until(fun, timeout_ms \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    wait_loop(fun, deadline)
  end

  defp wait_loop(fun, deadline) do
    cond do
      fun.() -> :ok
      System.monotonic_time(:millisecond) >= deadline -> flunk("condition not met in time")
      true -> Process.sleep(2) && wait_loop(fun, deadline)
    end
  end
end
