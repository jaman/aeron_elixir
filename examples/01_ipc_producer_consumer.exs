Code.require_file("support.exs", __DIR__)
alias Examples.Support

client = Support.start_client()
channel = Support.ipc()
stream_id = Support.stream_id()

{:ok, subscription} = AeronElixir.add_subscription(client, channel, stream_id)
{:ok, publication} = AeronElixir.add_publication(client, channel, stream_id)
:ok = AeronElixir.await_connected(publication)

{:ok, position} = AeronElixir.publish(publication, "hello from the producer")
IO.puts("published at position #{position}")

[message] = Support.poll_until(subscription, 1)
IO.puts("consumer received: #{message}")

{:ok, _} = AeronElixir.close(publication)
{:ok, _} = AeronElixir.close(subscription)
