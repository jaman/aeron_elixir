require Logger
Code.require_file("rpc_shared.exs", __DIR__)

alias AeronElixir.Bench.Rpc
alias AeronElixir.Bench.Shared

Logger.configure(level: :warning)
{:ok, _} = Application.ensure_all_started(:aeron_elixir)

client = Shared.start_client()
pair_index = String.to_integer(System.get_env("RPC_PAIR_INDEX", "0"))

{handle, sub} = Rpc.ping_side(client, pair_index)
image = Rpc.await_image(sub, 60_000)
stats = Rpc.run_pinger(handle, image, Rpc.warmup_messages(), Rpc.messages())
Rpc.print_json("elixir", [{"pairs", 1}, {"pair_index", pair_index}], stats)
