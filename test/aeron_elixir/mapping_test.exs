defmodule AeronElixir.MappingTest do
  use ExUnit.Case

  alias AeronElixir.MediaDriver

  @moduletag :system

  describe "mapped region lifetime" do
    setup do
      {:ok, client} = start_supervised(AeronElixir)
      %{client: client}
    end

    test "adding a publication and a subscription maps regions", %{client: client} do
      before = AeronElixir.mapped_region_count()

      channel = "aeron:ipc?alias=map-#{:erlang.unique_integer([:positive])}"
      stream_id = :erlang.unique_integer([:positive])

      {:ok, publication} = AeronElixir.add_publication(client, channel, stream_id)
      {:ok, subscription} = AeronElixir.add_subscription(client, channel, stream_id)
      :ok = AeronElixir.await_connected(subscription, 5_000)

      assert AeronElixir.mapped_region_count() > before

      AeronElixir.close(publication)
      AeronElixir.close(subscription)
    end
  end

  @doc """
  A mapping is released once every reference to it is collected.

  The conductor holds each handle in its own state and in the client's
  `RuntimeHandles` table, so closing is what drops those two references; the
  destructor then runs when the processes that handled those terms next collect,
  which this test forces rather than waits for. The driver is this test's own, so
  the count it reads moves only with its own publications and images.
  """
  test "closing and collecting returns the count to its starting value" do
    {:ok, driver, client} = own_driver("ae_drop")

    before = AeronElixir.mapped_region_count()
    channel = "aeron:ipc?alias=drop"
    stream_id = :erlang.unique_integer([:positive])

    {:ok, publication} = AeronElixir.add_publication(client, channel, stream_id)
    {:ok, subscription} = AeronElixir.add_subscription(client, channel, stream_id)
    :ok = AeronElixir.await_connected(subscription, 5_000)

    assert AeronElixir.mapped_region_count() > before

    AeronElixir.close(publication)
    AeronElixir.close(subscription)

    assert eventually_collected(before)

    :ok = MediaDriver.stop(driver)
  end

  @doc """
  Repeated open and close cycles release every mapping they take.

  The cycles run against a driver this test owns: the volume of driver responses
  they produce would otherwise lap the to-clients broadcast buffer that every
  client of a shared driver reads.
  """
  test "opening and closing repeatedly does not accumulate mappings" do
    {:ok, driver, client} = own_driver("ae_churn")
    channel = "aeron:ipc?alias=churn"

    for _ <- 1..20, do: open_and_close(client, channel)
    settled = settle()

    for _ <- 1..200, do: open_and_close(client, channel)

    assert eventually_collected(settled)

    :ok = MediaDriver.stop(driver)
  end

  defp own_driver(prefix) do
    aeron_dir = Path.join(System.tmp_dir!(), "#{prefix}_#{:erlang.unique_integer([:positive])}")
    {:ok, driver} = MediaDriver.start_link(aeron_dir: aeron_dir)
    on_exit(fn -> File.rm_rf(aeron_dir) end)
    {:ok, client} = AeronElixir.start_link(aeron_directory: aeron_dir, name: nil)
    {:ok, driver, client}
  end

  defp open_and_close(client, channel) do
    client
    |> AeronElixir.add_publication(channel, :erlang.unique_integer([:positive]))
    |> close_publication()
  end

  defp close_publication({:ok, publication}), do: AeronElixir.close(publication)
  defp close_publication({:error, _reason}), do: :ok

  defp settle do
    collect_all()
    Process.sleep(50)
    AeronElixir.mapped_region_count()
  end

  defp eventually_collected(target, attempts \\ 40)

  defp eventually_collected(target, 0), do: AeronElixir.mapped_region_count() <= target

  defp eventually_collected(target, attempts) do
    collect_all()
    settled_at_or_below(AeronElixir.mapped_region_count() <= target, target, attempts)
  end

  defp settled_at_or_below(true, _target, _attempts), do: true

  defp settled_at_or_below(false, target, attempts) do
    Process.sleep(25)
    eventually_collected(target, attempts - 1)
  end

  defp collect_all, do: Enum.each(Process.list(), &:erlang.garbage_collect/1)
end
