require Logger
Code.require_file("../shared.exs", __DIR__)

alias AeronElixir.Bench.Shared
alias AeronElixir.LogBuffer.Publisher
alias AeronElixir.LogBuffer.Subscriber

Logger.configure(level: :warning)
{:ok, _} = Application.ensure_all_started(:aeron_elixir)

defmodule MultiProc do
  @batch 1_000
  @poll_limit 1_024

  def run_pair(parent, runtime, images, payload, mode, warmup_ns, time_ns) do
    warmup_deadline = System.monotonic_time(:nanosecond) + warmup_ns
    loop(runtime, images, payload, mode, warmup_deadline, 0, 0, 0)
    drain_until_quiet(images, @batch, 0)

    start = System.monotonic_time(:nanosecond)
    {sent, received, tick_sum} = loop(runtime, images, payload, mode, start + time_ns, 0, 0, 0)
    elapsed = System.monotonic_time(:nanosecond) - start
    received = received + drain_until_quiet(images, sent - received, 0)
    send(parent, {:done, sent, received, elapsed, tick_sum})
  end

  defp loop(runtime, images, payload, mode, deadline, sent, received, tick_sum) do
    if System.monotonic_time(:nanosecond) >= deadline do
      {sent, received, tick_sum}
    else
      {drained_in_send, sum_in_send} = send_batch(mode, runtime, images, payload, sent, @batch, 0, 0)
      {drained, sum} = drain(images, 0, 0)

      loop(
        runtime,
        images,
        payload,
        mode,
        deadline,
        sent + @batch,
        received + drained_in_send + drained,
        tick_sum + sum_in_send + sum
      )
    end
  end

  defp send_batch(_mode, _runtime, _images, _payload, _sequence, 0, drained, sum), do: {drained, sum}

  defp send_batch(:per_message, runtime, images, payload, sequence, remaining, drained, sum) do
    case Publisher.publish(runtime, Shared.message(payload, sequence), 0) do
      {:ok, _} ->
        send_batch(:per_message, runtime, images, payload, sequence + 1, remaining - 1, drained, sum)

      {:error, _} ->
        {drained_now, sum_now} = drain(images, 0, 0)

        send_batch(
          :per_message,
          runtime,
          images,
          payload,
          sequence,
          remaining,
          drained + drained_now,
          sum + sum_now
        )
    end
  end

  defp send_batch(:list, runtime, images, payload, sequence, remaining, drained, sum) do
    payloads = for offset <- 0..(remaining - 1), do: Shared.message(payload, sequence + offset)
    published = Publisher.publish_list(runtime, payloads, 0)

    if published < remaining do
      {drained_now, sum_now} = drain(images, 0, 0)

      send_batch(
        :list,
        runtime,
        images,
        payload,
        sequence + published,
        remaining - published,
        drained + drained_now,
        sum + sum_now
      )
    else
      {drained, sum}
    end
  end

  defp drain(images, count, sum) do
    {polled, polled_sum} =
      Enum.reduce(images, {0, 0}, fn image, {acc_count, acc_sum} ->
        {n, payloads} = Subscriber.collect(image, @poll_limit)
        {acc_count + n, Enum.reduce(payloads, acc_sum, &Shared.tick_sum/2)}
      end)

    if polled == 0, do: {count, sum}, else: drain(images, count + polled, sum + polled_sum)
  end

  defp drain_until_quiet(_images, outstanding, acc) when outstanding <= 0, do: acc

  defp drain_until_quiet(images, outstanding, acc) do
    case drain(images, 0, 0) do
      {0, _} -> acc
      {n, _} -> drain_until_quiet(images, outstanding - n, acc + n)
    end
  end
end

pairs = String.to_integer(System.get_env("BENCH_PAIRS", "4"))

{mode, label} =
  case System.get_env("BENCH_PUBLISH_MODE", "per_message") do
    "list" -> {:list, "elixir_list"}
    _ -> {:per_message, "elixir"}
  end

warmup_ns = Shared.warmup_seconds() * 1_000_000_000
time_ns = Shared.time_seconds() * 1_000_000_000

client = Shared.start_client()

for size <- Shared.payload_sizes() do
  payload = Shared.payload_pool(size)

  setups =
    for _ <- 1..pairs do
      channel = Shared.unique_channel("mp")
      stream_id = Shared.unique_stream_id()
      {pub, sub} = Shared.setup_pub_sub(client, channel, stream_id)
      Shared.await_first_message(pub, sub, payload)
      {:ok, runtime} = AeronElixir.publication_handle(pub)
      {:ok, images} = AeronElixir.subscription_handles(sub)
      {runtime, images}
    end

  parent = self()

  for {runtime, images} <- setups do
    spawn_link(fn ->
      MultiProc.run_pair(parent, runtime, images, payload, mode, warmup_ns, time_ns)
    end)
  end

  results =
    for _ <- 1..pairs do
      receive do
        {:done, sent, received, elapsed, tick_sum} -> {sent, received, elapsed, tick_sum}
      end
    end

  total_sent = results |> Enum.map(&elem(&1, 0)) |> Enum.sum()
  total_received = results |> Enum.map(&elem(&1, 1)) |> Enum.sum()
  max_elapsed = results |> Enum.map(&elem(&1, 2)) |> Enum.max()
  tick_sum = results |> Enum.map(&elem(&1, 3)) |> Enum.sum()
  ops = total_sent * 1_000_000_000.0 / max_elapsed

  IO.puts(:stderr, "[bench-elixir-mp] #{label} tick_sum=#{tick_sum}")

  IO.puts(
    "{\"client\":\"#{label}\",\"scenario\":\"throughput\"," <>
      "\"payload_size\":#{size},\"workers\":#{pairs}," <>
      "\"samples\":#{total_sent},\"received\":#{total_received}," <>
      "\"ops_per_sec\":#{ops},\"bytes_per_sec\":#{ops * size}}"
  )
end
