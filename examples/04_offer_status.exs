Code.require_file("support.exs", __DIR__)
alias Examples.Support

client = Support.start_client()
channel = Support.ipc(term_length: 65_536)
stream_id = Support.stream_id()

{:ok, publication} = AeronElixir.add_publication(client, channel, stream_id)
IO.puts("before a subscriber exists: #{inspect(AeronElixir.try_publish(publication, "early"))}")

{:ok, subscription} = AeronElixir.add_subscription(client, channel, stream_id)
:ok = AeronElixir.await_connected(publication)

payload = :binary.copy(<<0>>, 4096)

statuses =
  Stream.repeatedly(fn -> AeronElixir.try_publish(publication, payload) end)
  |> Enum.take(64)
  |> Enum.frequencies_by(fn
    {:ok, _position} -> :ok
    {:error, status} -> status
  end)

IO.puts("64 attempts against a 64 KiB term without draining: #{inspect(statuses)}")
IO.puts("position #{AeronElixir.position(publication)}, limit #{AeronElixir.position_limit(publication)}")

_drained = Support.poll_until(subscription, statuses[:ok])
IO.puts("after draining: #{inspect(AeronElixir.try_publish(publication, "late"))}")

{:ok, _} = AeronElixir.close(publication)
IO.puts("after close: #{inspect(AeronElixir.try_publish(publication, "closed"))}")
{:ok, _} = AeronElixir.close(subscription)
