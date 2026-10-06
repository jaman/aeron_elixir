require Logger
Code.require_file("rpc_shared.exs", __DIR__)

alias AeronElixir.Bench.Rpc
alias AeronElixir.Bench.Shared

Logger.configure(level: :warning)
{:ok, _} = Application.ensure_all_started(:aeron_elixir)

client = Shared.start_client()
pairs = Rpc.pairs()
parent = self()

sides =
  for pair_index <- 0..(pairs - 1) do
    pong = Rpc.pong_side(client, pair_index)
    ping = Rpc.ping_side(client, pair_index)
    {pair_index, ping, pong}
  end

for {_pair_index, _ping, {pong_handle, pong_sub}} <- sides do
  spawn_link(fn ->
    image = Rpc.await_image(pong_sub, 60_000)
    Rpc.pong_loop(pong_handle, image)
  end)
end

for {pair_index, {ping_handle, ping_sub}, _pong} <- sides do
  spawn_link(fn ->
    image = Rpc.await_image(ping_sub, 60_000)
    stats = Rpc.run_pinger(ping_handle, image, Rpc.warmup_messages(), Rpc.messages())
    send(parent, {:done, pair_index, stats})
  end)
end

results =
  for _ <- 1..pairs do
    receive do
      {:done, pair_index, stats} -> {pair_index, stats}
    end
  end

for {pair_index, stats} <- results do
  Rpc.print_json("elixir_invm", [{"pairs", pairs}, {"pair_index", pair_index}], stats)
end
