Code.require_file("../shared.exs", __DIR__)

alias AeronElixir.Bench.Shared
alias AeronElixir.LogBuffer.Publisher
alias AeronElixir.LogBuffer.Subscriber

defmodule UdpBench do
  @moduledoc false

  def publish_until_accepted(runtime, images, payload) do
    case Publisher.publish(runtime, payload, 0) do
      {:ok, _position} ->
        :ok

      {:error, _retryable} ->
        drain(images, 0)
        publish_until_accepted(runtime, images, payload)
    end
  end

  def drain(images, acc) do
    polled =
      Enum.reduce(images, 0, fn image, count ->
        {read, _payloads} = Subscriber.collect(image, 1024)
        count + read
      end)

    case polled do
      0 -> acc
      n -> drain(images, acc + n)
    end
  end

  def drain_one(images) do
    case drain(images, 0) do
      0 -> drain_one(images)
      n -> n
    end
  end

  def throughput(runtime, images, payload, deadline, sent) do
    case System.monotonic_time(:nanosecond) >= deadline do
      true ->
        sent

      false ->
        publish_until_accepted(runtime, images, Shared.message(payload, sent))
        drain(images, 0)
        throughput(runtime, images, payload, deadline, sent + 1)
    end
  end


  def latency_loop(runtime, images, payload, deadline, samples) do
    case System.monotonic_time(:nanosecond) >= deadline do
      true ->
        samples

      false ->
        start = System.monotonic_time(:nanosecond)
        publish_until_accepted(runtime, images, Shared.message(payload, System.unique_integer([:positive, :monotonic])))
        drain_one(images)
        elapsed = System.monotonic_time(:nanosecond) - start
        latency_loop(runtime, images, payload, deadline, [elapsed | samples])
    end
  end

  def percentile(sorted, pct) do
    index = min(trunc(pct / 100 * length(sorted)), length(sorted) - 1)
    Enum.at(sorted, index)
  end
end

{:ok, _} = Application.ensure_all_started(:aeron_elixir)

media = System.get_env("BENCH_MEDIA", "udp")
warmup_s = Shared.warmup_seconds()
time_s = Shared.time_seconds()

client = Shared.start_client()

for size <- Shared.payload_sizes() do
  channel =
    case media do
      "udp" ->
        port = 41_000 + :erlang.phash2({System.pid(), System.system_time(), size}, 15_000)
        "aeron:udp?endpoint=127.0.0.1:#{port}|term-length=#{Shared.term_length()}"

      _ ->
        Shared.unique_channel("udpbench")
    end

  stream_id = Shared.unique_stream_id()
  {pub, sub} = Shared.setup_pub_sub(client, channel, stream_id)
  payload = Shared.payload_pool(size)

  :ok = AeronElixir.await_connected(pub, 10_000)
  :ok = AeronElixir.await_connected(sub, 10_000)
  Shared.await_first_message(pub, sub, payload)

  {:ok, runtime} = AeronElixir.publication_handle(pub)
  {:ok, images} = AeronElixir.subscription_handles(sub)

  UdpBench.throughput(
    runtime,
    images,
    payload,
    System.monotonic_time(:nanosecond) + warmup_s * 1_000_000_000,
    0
  )

  UdpBench.drain(images, 0)

  start_ns = System.monotonic_time(:nanosecond)

  sent =
    UdpBench.throughput(runtime, images, payload, start_ns + time_s * 1_000_000_000, 0)

  elapsed_ns = System.monotonic_time(:nanosecond) - start_ns
  UdpBench.drain(images, 0)

  ops = sent * 1_000_000_000.0 / elapsed_ns

  UdpBench.latency_loop(
    runtime,
    images,
    payload,
    System.monotonic_time(:nanosecond) + warmup_s * 1_000_000_000,
    []
  )

  samples =
    UdpBench.latency_loop(
      runtime,
      images,
      payload,
      System.monotonic_time(:nanosecond) + time_s * 1_000_000_000,
      []
    )

  sorted = Enum.sort(samples)

  IO.puts(
    "{\"client\":\"elixir_#{media}\",\"payload_size\":#{size}," <>
      "\"throughput_ops_per_sec\":#{Float.round(ops, 1)}," <>
      "\"latency_samples\":#{length(sorted)}," <>
      "\"p50_us\":#{Float.round(UdpBench.percentile(sorted, 50) / 1000, 3)}," <>
      "\"p99_us\":#{Float.round(UdpBench.percentile(sorted, 99) / 1000, 3)}," <>
      "\"p999_us\":#{Float.round(UdpBench.percentile(sorted, 99.9) / 1000, 3)}}"
  )

  AeronElixir.close(pub)
  AeronElixir.close(sub)
end
