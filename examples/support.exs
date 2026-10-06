defmodule Examples.Support do
  @moduledoc """
  Helpers shared by the example scripts: a client on the driver
  `AeronElixir.DriverConfig` selects (the application's embedded driver unless
  `AERON_DIR` names another), unique stream ids, and bounded polling loops.
  """

  alias AeronElixir.Idle

  def start_client do
    {:ok, _} = Application.ensure_all_started(:aeron_elixir)
    {:ok, client} = AeronElixir.start_link(name: nil)
    client
  end

  def stream_id, do: :erlang.unique_integer([:positive])

  def ipc(params \\ []) do
    AeronElixir.ChannelUri.ipc([alias: "example-#{stream_id()}"] ++ params)
  end

  def poll_until(subscription, target, timeout_ms \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    poll_loop(subscription, target, deadline, [], Idle.backoff())
  end

  defp poll_loop(subscription, target, deadline, acc, idle) do
    {:ok, count, payloads} = AeronElixir.poll_batch(subscription, 64)
    acc = acc ++ payloads

    cond do
      length(acc) >= target -> acc
      System.monotonic_time(:millisecond) >= deadline -> acc
      true -> poll_loop(subscription, target, deadline, acc, Idle.idle(idle, count))
    end
  end

  def wait_until(condition, timeout_ms \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    wait_loop(condition, deadline)
  end

  defp wait_loop(condition, deadline) do
    cond do
      condition.() -> :ok
      System.monotonic_time(:millisecond) >= deadline -> {:error, :timeout}
      true -> Process.sleep(2) && wait_loop(condition, deadline)
    end
  end
end
