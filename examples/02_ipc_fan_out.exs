Code.require_file("support.exs", __DIR__)
alias Examples.Support

client = Support.start_client()
channel = Support.ipc()
stream_id = Support.stream_id()

{:ok, first} = AeronElixir.add_subscription(client, channel, stream_id)
{:ok, second} = AeronElixir.add_subscription(client, channel, stream_id)
{:ok, publication} = AeronElixir.add_publication(client, channel, stream_id)
:ok = AeronElixir.await_connected(publication)

messages = for n <- 1..3, do: "tick #{n}"
{:ok, 3} = AeronElixir.publish_list(publication, messages)

IO.puts("first consumer:  #{inspect(Support.poll_until(first, 3))}")
IO.puts("second consumer: #{inspect(Support.poll_until(second, 3))}")

for resource <- [publication, first, second], do: {:ok, _} = AeronElixir.close(resource)
