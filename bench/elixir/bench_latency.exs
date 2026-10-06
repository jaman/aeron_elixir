Code.require_file("../shared.exs", __DIR__)

alias AeronElixir.Bench.Shared

defmodule LatencyBench do
  @moduledoc """
  Offer-to-poll round trip of one message at a time, the same loop the native
  benches run: each sample stamps the tick into the next pool entry, offers it
  until accepted and polls until it arrives, without decoding it. Samples are
  timed with the monotonic clock and recorded in a histogram of 1 ns buckets up
  to 100 µs; slower samples count toward the maximum and the mean only.

  `BENCH_PUBLISH_MODE=direct` uses the handle API (`Publisher.publish/3` and
  `Subscriber.next/1`); every other mode uses the public API
  (`AeronElixir.publish/2` and `AeronElixir.poll/3` with a limit of 1).
  """

  alias AeronElixir.Bench.Shared
  alias AeronElixir.LogBuffer.{Publisher, Subscriber}

  @buckets 100_000

  def round_trip("direct", publication, subscription) do
    {:ok, handle} = AeronElixir.publication_handle(publication)
    image = await_image(subscription)

    fn message ->
      offer_until_accepted(handle, message)
      await_next(image)
    end
  end

  def round_trip(_mode, publication, subscription) do
    fn message ->
      {:ok, _position} = AeronElixir.publish(publication, message)
      await_polled(subscription)
    end
  end

  def run(round_trip, pool, warmup_ns, time_ns) do
    warm(round_trip, pool, 0, now() + warmup_ns)
    histogram = :atomics.new(@buckets, signed: false)
    start = now()

    {samples, total_ns, max_ns} =
      measure(round_trip, pool, 0, start + time_ns, histogram, {0, 0, 0})

    report(histogram, samples, total_ns, max_ns, now() - start)
  end

  defp await_image(subscription) do
    case AeronElixir.subscription_handles(subscription) do
      {:ok, [image | _]} ->
        image

      _none ->
        Process.sleep(10)
        await_image(subscription)
    end
  end

  defp offer_until_accepted(handle, message) do
    case Publisher.publish(handle, message, 0) do
      {:ok, _position} ->
        :ok

      {:error, reason} when reason in [:closed, :max_position_exceeded] ->
        raise "publication #{reason}"

      {:error, _retryable} ->
        offer_until_accepted(handle, message)
    end
  end

  defp await_next(image), do: await_next(Subscriber.next(image), image)
  defp await_next(nil, image), do: await_next(Subscriber.next(image), image)
  defp await_next(_payload, _image), do: :ok

  defp await_polled(subscription) do
    case AeronElixir.poll(subscription, 1, &count_fragment/2) do
      0 -> await_polled(subscription)
      _received -> :ok
    end
  end

  defp count_fragment(_payload, _header), do: :ok

  defp warm(round_trip, pool, sequence, deadline) do
    if now() < deadline do
      round_trip.(Shared.message(pool, sequence))
      warm(round_trip, pool, sequence + 1, deadline)
    end
  end

  defp measure(
         round_trip,
         pool,
         sequence,
         deadline,
         histogram,
         {samples, total, max_ns} = acc
       ) do
    started = now()

    if started >= deadline do
      acc
    else
      round_trip.(Shared.message(pool, sequence))
      elapsed = now() - started
      record(histogram, elapsed)
      next = {samples + 1, total + elapsed, max(max_ns, elapsed)}
      measure(round_trip, pool, sequence + 1, deadline, histogram, next)
    end
  end

  defp record(histogram, elapsed) when elapsed < @buckets,
    do: :atomics.add(histogram, elapsed + 1, 1)

  defp record(_histogram, _elapsed), do: :ok

  defp report(histogram, samples, total_ns, max_ns, elapsed_ns) do
    %{
      samples: samples,
      ops_per_sec: samples * 1.0e9 / elapsed_ns,
      elapsed_ms: elapsed_ns / 1.0e6,
      mean_us: total_ns / max(samples, 1) / 1000,
      median_us: percentile_us(histogram, samples, 0.5),
      p99_us: percentile_us(histogram, samples, 0.99),
      p999_us: percentile_us(histogram, samples, 0.999),
      min_us: percentile_us(histogram, samples, 0.0),
      max_us: max_ns / 1000
    }
  end

  defp percentile_us(histogram, samples, fraction),
    do: find_bucket(histogram, max(trunc(samples * fraction), 1), 1, 0)

  defp find_bucket(_histogram, _target, index, _seen) when index > @buckets, do: @buckets / 1000

  defp find_bucket(histogram, target, index, seen) do
    seen = seen + :atomics.get(histogram, index)

    if seen >= target,
      do: (index - 1) / 1000,
      else: find_bucket(histogram, target, index + 1, seen)
  end

  defp now, do: System.monotonic_time(:nanosecond)
end

{:ok, _} = Application.ensure_all_started(:aeron_elixir)

client = Shared.start_client()
mode = System.get_env("BENCH_PUBLISH_MODE", "public")
warmup_ns = Shared.warmup_seconds() * 1_000_000_000
time_ns = Shared.time_seconds() * 1_000_000_000

for size <- Shared.payload_sizes() do
  {publication, subscription} =
    Shared.setup_pub_sub(client, Shared.unique_channel("latency"), Shared.unique_stream_id())

  pool = Shared.payload_pool(size)
  Shared.await_first_message(publication, subscription, pool)

  stats =
    mode
    |> LatencyBench.round_trip(publication, subscription)
    |> LatencyBench.run(pool, warmup_ns, time_ns)

  fields =
    [client: Shared.client_label(), scenario: "latency", payload_size: size] ++
      Enum.sort(Map.to_list(stats))

  IO.puts(Jason.encode!(Jason.OrderedObject.new(fields)))

  AeronElixir.close(publication)
  AeronElixir.close(subscription)
end

AeronElixir.close(AeronElixir.client(client))
