Code.require_file("support.exs", __DIR__)
alias AeronElixir.MediaDriver
alias Examples.Support

{:ok, _} = Application.ensure_all_started(:aeron_elixir)

aeron_dir = Path.join(AeronElixir.DriverConfig.private_base(), "example-lifecycle-#{System.pid()}")
{:ok, driver} = MediaDriver.start_link(aeron_dir: aeron_dir, remove_directory: true)
IO.puts("started a second media driver, pid #{MediaDriver.os_pid(driver)}, in #{aeron_dir}")

{:ok, client_server} = AeronElixir.start_link(aeron_directory: aeron_dir, name: nil)
client = AeronElixir.client(client_server)
IO.puts("client #{client.id} connected to #{aeron_dir}")

channel = Support.ipc()
stream_id = Support.stream_id()
{:ok, subscription} = AeronElixir.add_subscription(client, channel, stream_id)
{:ok, publication} = AeronElixir.add_publication(client, channel, stream_id)

IO.puts("publication connected? #{AeronElixir.connected?(publication)}")
:ok = AeronElixir.await_connected(publication)
:ok = AeronElixir.await_connected(subscription)
IO.puts("publication connected? #{AeronElixir.connected?(publication)}, subscription connected? #{AeronElixir.connected?(subscription)}")

{:ok, _} = AeronElixir.publish(publication, "one full session")
IO.puts("received #{inspect(Support.poll_until(subscription, 1))}")

{:ok, closed_publication} = AeronElixir.close(publication)
{:ok, closed_subscription} = AeronElixir.close(subscription)
IO.puts("publication closed? #{AeronElixir.closed?(closed_publication)}, subscription closed? #{AeronElixir.closed?(closed_subscription)}")
IO.puts("publish after close: #{inspect(AeronElixir.try_publish(publication, "late"))}")

{:ok, closed_client} = AeronElixir.close(client)
IO.puts("client closed? #{AeronElixir.closed?(closed_client)}")

:ok = MediaDriver.stop(driver)
IO.puts("media driver stopped")
