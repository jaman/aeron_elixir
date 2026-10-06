Code.require_file("support.exs", __DIR__)
alias Examples.Support

client = Support.start_client()
channel = Support.ipc()
orders_stream = Support.stream_id()
quotes_stream = Support.stream_id()

{:ok, orders} = AeronElixir.add_subscription(client, channel, orders_stream)
{:ok, quotes} = AeronElixir.add_subscription(client, channel, quotes_stream)
{:ok, order_pub} = AeronElixir.add_publication(client, channel, orders_stream)
{:ok, quote_pub} = AeronElixir.add_publication(client, channel, quotes_stream)
:ok = AeronElixir.await_connected(order_pub)
:ok = AeronElixir.await_connected(quote_pub)

{:ok, _} = AeronElixir.publish(order_pub, "BUY 100 XYZ")
{:ok, _} = AeronElixir.publish(quote_pub, "XYZ 10.00/10.05")
{:ok, _} = AeronElixir.publish(order_pub, "SELL 50 XYZ")

IO.puts("orders stream #{orders_stream}: #{inspect(Support.poll_until(orders, 2))}")
IO.puts("quotes stream #{quotes_stream}: #{inspect(Support.poll_until(quotes, 1))}")

for resource <- [order_pub, quote_pub, orders, quotes], do: {:ok, _} = AeronElixir.close(resource)
