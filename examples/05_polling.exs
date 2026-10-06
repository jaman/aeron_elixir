Code.require_file("support.exs", __DIR__)
alias AeronElixir.Idle
alias Examples.Support

client = Support.start_client()
channel = Support.ipc()
stream_id = Support.stream_id()

{:ok, subscription} = AeronElixir.add_subscription(client, channel, stream_id)
{:ok, publication} = AeronElixir.add_publication(client, channel, stream_id)
:ok = AeronElixir.await_connected(publication)

small = for n <- 1..5, do: "small #{n}"
large = :binary.copy(<<?L>>, publication.max_payload_length * 3)
{:ok, 5} = AeronElixir.publish_list(publication, small)
{:ok, _} = AeronElixir.publish(publication, large)

handler = fn payload, header ->
  session = AeronElixir.Header.session_id(header)
  IO.puts("  #{byte_size(payload)} bytes, session #{session}, position #{AeronElixir.Header.position(header)}")
end

fragments = div(byte_size(large), publication.max_payload_length)
IO.puts("duty cycle with fragment_limit 2; the large message is reassembled from #{fragments} fragments:")

Enum.reduce_while(1..1_000, {0, Idle.backoff()}, fn _, {delivered, idle} ->
  count = AeronElixir.poll(subscription, 2, handler)
  delivered = delivered + count
  if delivered >= 6, do: {:halt, delivered}, else: {:cont, {delivered, Idle.idle(idle, count)}}
end)

{:ok, _} = AeronElixir.publish(publication, "kept")
[kept] = Support.poll_until(subscription, 1)
IO.puts("payloads are plain binaries that outlive the poll: #{inspect(kept)}")

{:ok, _} = AeronElixir.close(publication)
{:ok, _} = AeronElixir.close(subscription)
