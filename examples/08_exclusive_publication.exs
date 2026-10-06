Code.require_file("support.exs", __DIR__)
alias Examples.Support

client = Support.start_client()
channel = Support.ipc()
stream_id = Support.stream_id()

{:ok, subscription} = AeronElixir.add_subscription(client, channel, stream_id)
{:ok, publication} = AeronElixir.add_exclusive_publication(client, channel, stream_id)
{:ok, other} = AeronElixir.add_exclusive_publication(client, channel, stream_id)
:ok = AeronElixir.await_connected(publication)
:ok = AeronElixir.await_connected(other)

IO.puts("exclusive: #{publication.is_exclusive}, sessions #{publication.session_id} and #{other.session_id}")
{:ok, _} = AeronElixir.publish(publication, "from the exclusive publisher")
{:ok, _} = AeronElixir.publish(other, "from another session")

Support.wait_until(fn -> AeronElixir.image_count(subscription) == 2 end)
IO.puts("subscription sees #{AeronElixir.image_count(subscription)} images")
IO.puts("received: #{inspect(Support.poll_until(subscription, 2))}")

for resource <- [publication, other, subscription], do: {:ok, _} = AeronElixir.close(resource)
