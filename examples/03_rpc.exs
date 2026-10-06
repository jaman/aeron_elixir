Code.require_file("support.exs", __DIR__)
alias Examples.Support

client = Support.start_client()
channel = Support.ipc()
request_stream = Support.stream_id()
response_stream = Support.stream_id()

{:ok, requests} = AeronElixir.add_subscription(client, channel, request_stream)
{:ok, responses} = AeronElixir.add_subscription(client, channel, response_stream)
{:ok, request_pub} = AeronElixir.add_publication(client, channel, request_stream)
{:ok, response_pub} = AeronElixir.add_publication(client, channel, response_stream)
:ok = AeronElixir.await_connected(request_pub)
:ok = AeronElixir.await_connected(response_pub)

responder =
  Task.async(fn ->
    [<<correlation_id::64, body::binary>>] = Support.poll_until(requests, 1)
    {:ok, _} = AeronElixir.publish(response_pub, [<<correlation_id::64>>, "echo: ", body])
    correlation_id
  end)

correlation_id = :erlang.unique_integer([:positive])
{:ok, _} = AeronElixir.publish(request_pub, [<<correlation_id::64>>, "ping"])
IO.puts("sent request #{correlation_id}")

[<<^correlation_id::64, reply::binary>>] = Support.poll_until(responses, 1)
IO.puts("correlated response #{correlation_id}: #{reply}")
^correlation_id = Task.await(responder)

for resource <- [request_pub, response_pub, requests, responses],
    do: {:ok, _} = AeronElixir.close(resource)
