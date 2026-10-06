Code.require_file("support.exs", __DIR__)
alias Examples.Support

client = Support.start_client()
port = 42_000 + :erlang.phash2({System.pid(), System.system_time()}, 10_000)
channel = AeronElixir.ChannelUri.udp(endpoint: "127.0.0.1:#{port}")
stream_id = Support.stream_id()

IO.puts("channel: #{channel}")

{:ok, subscription} = AeronElixir.add_subscription(client, channel, stream_id)
{:ok, publication} = AeronElixir.add_publication(client, channel, stream_id)
:ok = AeronElixir.await_connected(publication)
:ok = AeronElixir.await_connected(subscription)

{:ok, position} = AeronElixir.publish(publication, "hello over udp")
IO.puts("published at position #{position}")

[message] = Support.poll_until(subscription, 1)
IO.puts("consumer received: #{message}")

{:ok, _} = AeronElixir.close(publication)
{:ok, _} = AeronElixir.close(subscription)
