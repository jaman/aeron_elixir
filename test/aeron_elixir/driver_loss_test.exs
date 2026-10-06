defmodule AeronElixir.DriverLossTest do
  use ExUnit.Case

  alias AeronElixir.MediaDriver

  @moduletag :system

  setup do
    aeron_dir = Path.join(System.tmp_dir!(), "ae_loss_#{:erlang.unique_integer([:positive])}")
    {:ok, driver} = GenServer.start(MediaDriver, aeron_dir: aeron_dir, remove_directory: true)
    on_exit(fn -> File.rm_rf!(aeron_dir) end)

    {:ok, client} =
      AeronElixir.start_link(aeron_directory: aeron_dir, name: nil, driver_timeout_ms: 1_000)

    Process.unlink(client)

    channel = "aeron:ipc?alias=loss-#{:erlang.unique_integer([:positive])}"
    {:ok, subscription} = AeronElixir.add_subscription(client, channel, 11)
    {:ok, publication} = AeronElixir.add_publication(client, channel, 11)
    :ok = AeronElixir.await_connected(publication)
    {:ok, handle} = AeronElixir.publication_handle(publication)
    {:ok, [image]} = AeronElixir.subscription_handles(subscription)

    %{
      driver: driver,
      client: client,
      publication: publication,
      subscription: subscription,
      handle: handle,
      image: image
    }
  end

  test "a killed driver is detected within the driver timeout and closes every handle", ctx do
    ref = Process.monitor(ctx.client)
    System.cmd("kill", ["-KILL", Integer.to_string(MediaDriver.os_pid(ctx.driver))])

    assert_receive {:DOWN, ^ref, :process, _client, {:shutdown, :driver_timeout}}, 3_000
    assert {:error, :closed} = AeronElixir.try_publish(ctx.handle, "after loss")
    assert {:error, :closed} = AeronElixir.publish_list(ctx.handle, ["after loss"])
    assert {:error, :closed} = AeronElixir.publish(ctx.publication, "after loss")
    assert {:ok, 0, []} = AeronElixir.poll_batch(ctx.image, 10)
    assert {:error, :closed} = AeronElixir.poll_batch(ctx.subscription, 10)
    assert AeronElixir.closed?(ctx.handle)
    assert AeronElixir.closed?(ctx.image)
  end

  test "a driver that shuts down is reported as a shutdown", ctx do
    ref = Process.monitor(ctx.client)
    :ok = MediaDriver.stop(ctx.driver)

    assert_receive {:DOWN, ^ref, :process, _client, {:shutdown, :driver_shutdown}}, 3_000
    assert {:error, :closed} = AeronElixir.try_publish(ctx.handle, "after shutdown")
  end
end
