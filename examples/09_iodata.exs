Code.require_file("support.exs", __DIR__)
alias Examples.Support

client = Support.start_client()
channel = Support.ipc()
stream_id = Support.stream_id()

{:ok, subscription} = AeronElixir.add_subscription(client, channel, stream_id)
{:ok, publication} = AeronElixir.add_publication(client, channel, stream_id)
:ok = AeronElixir.await_connected(publication)

header = <<1::32, 2::64>>
shared_tail = :binary.copy(<<?t>>, 64)

payloads = [
  "a binary",
  [header, shared_tail],
  ["nested ", ["iodata ", ?!]],
  <<3::little-32, 4::little-64>>
]

{:ok, 4} = AeronElixir.publish_list(publication, payloads)

for received <- Support.poll_until(subscription, 4) do
  IO.puts("#{byte_size(received)} bytes: #{inspect(received, limit: 12)}")
end

{:ok, _} = AeronElixir.close(publication)
{:ok, _} = AeronElixir.close(subscription)
