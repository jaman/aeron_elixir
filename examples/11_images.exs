Code.require_file("support.exs", __DIR__)
alias Examples.Support

client = Support.start_client()
channel = Support.ipc()
stream_id = Support.stream_id()

parent = self()

{:ok, subscription} =
  AeronElixir.add_subscription(client, channel, stream_id,
    available_image: fn image -> send(parent, {:available, image.session_id}) end,
    unavailable_image: fn image -> send(parent, {:unavailable, image.session_id}) end
  )

{:ok, first} = AeronElixir.add_exclusive_publication(client, channel, stream_id)
{:ok, second} = AeronElixir.add_exclusive_publication(client, channel, stream_id)
:ok = AeronElixir.await_connected(first)
:ok = AeronElixir.await_connected(second)
:ok = Support.wait_until(fn -> AeronElixir.image_count(subscription) == 2 end)

for _ <- 1..2 do
  receive do
    {:available, session_id} -> IO.puts("image available for session #{session_id}")
  after
    5_000 -> IO.puts("no availability callback")
  end
end

{:ok, _} = AeronElixir.publish(first, "first session says hi")
{:ok, _} = AeronElixir.publish(second, "second session says hi")

{:ok, image} = AeronElixir.image_by_session_id(subscription, second.session_id)
IO.puts("polling only session #{image.session_id} (source #{inspect(image.source_identity)}) at position #{AeronElixir.image_position(image)}")

Support.wait_until(fn -> match?({:ok, 1, _}, AeronElixir.poll_image_batch(image, 8)) end)
IO.puts("image position now #{AeronElixir.image_position(image)}, end of stream? #{AeronElixir.end_of_stream?(image)}")

{:ok, other} = AeronElixir.image_by_session_id(subscription, first.session_id)
AeronElixir.poll_image(other, 8, fn payload, _header -> IO.puts("first session delivered: #{payload}") end)

{:ok, _} = AeronElixir.close(second)

receive do
  {:unavailable, session_id} -> IO.puts("image unavailable for session #{session_id}")
after
  15_000 -> IO.puts("no unavailability callback")
end

IO.puts("images left: #{AeronElixir.image_count(subscription)}")
for resource <- [first, subscription], do: {:ok, _} = AeronElixir.close(resource)
