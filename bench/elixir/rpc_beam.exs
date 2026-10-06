Code.require_file("rpc_shared.exs", __DIR__)

alias AeronElixir.Bench.Rpc

pairs = Rpc.pairs()
parent = self()

for pair_index <- 0..(pairs - 1) do
  ponger = spawn_link(fn -> Rpc.beam_ponger() end)

  spawn_link(fn ->
    stats = Rpc.beam_pinger(ponger, Rpc.warmup_messages(), Rpc.messages())
    send(ponger, :stop)
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
  Rpc.print_json("beam", [{"pairs", pairs}, {"pair_index", pair_index}], stats)
end
