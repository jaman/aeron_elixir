defmodule AeronElixir.MediaDriverTest do
  use ExUnit.Case

  alias AeronElixir.MediaDriver

  @moduletag :system

  test "starts an external driver in its own directory, serves a client, and stops it" do
    aeron_dir = Path.join(System.tmp_dir!(), "ae_md_#{:erlang.unique_integer([:positive])}")

    {:ok, driver} = MediaDriver.start_link(aeron_dir: aeron_dir)
    assert File.exists?(Path.join(aeron_dir, "cnc.dat"))
    assert MediaDriver.alive?(driver)

    {:ok, client} = start_supervised({AeronElixir, aeron_directory: aeron_dir, name: nil})
    channel = "aeron:ipc?alias=md-test"
    {:ok, publication} = AeronElixir.add_publication(client, channel, 7)
    {:ok, subscription} = AeronElixir.add_subscription(client, channel, 7)
    :ok = AeronElixir.await_connected(publication, 5_000)
    {:ok, _} = AeronElixir.publish(publication, "via embedded driver")
    assert {:ok, 1, ["via embedded driver"]} = poll_until(subscription, 200)

    os_pid = MediaDriver.os_pid(driver)
    :ok = stop_supervised(AeronElixir)
    :ok = MediaDriver.stop(driver)
    refute Process.alive?(driver)
    refute os_process_alive?(os_pid)
  end

  defp poll_until(_subscription, 0), do: {:ok, 0, []}

  defp poll_until(subscription, attempts) do
    case AeronElixir.poll_batch(subscription, 8) do
      {:ok, 0, []} ->
        Process.sleep(5)
        poll_until(subscription, attempts - 1)

      result ->
        result
    end
  end

  defp os_process_alive?(os_pid) do
    {output, _} = System.cmd("ps", ["-p", Integer.to_string(os_pid), "-o", "pid="])
    String.trim(output) != ""
  end
end
