Code.require_file("../shared.exs", __DIR__)

defmodule AeronElixir.Bench.Rpc do
  @moduledoc false

  alias AeronElixir.Bench.Shared
  alias AeronElixir.LogBuffer.Publisher
  alias AeronElixir.LogBuffer.Subscriber

  @channel "aeron:ipc"
  @start_spin_ns 2_000_000

  def message_length, do: String.to_integer(System.get_env("RPC_MESSAGE_LENGTH", "32"))
  def warmup_messages, do: String.to_integer(System.get_env("RPC_WARMUP_MESSAGES", "100000"))
  def messages, do: String.to_integer(System.get_env("RPC_MESSAGES", "1000000"))
  def stream_base, do: String.to_integer(System.get_env("RPC_STREAM_BASE", "7100"))
  def pairs, do: String.to_integer(System.get_env("BENCH_PAIRS", "1"))
  def start_at_ns, do: String.to_integer(System.get_env("RPC_START_AT_NS", "0"))

  def await_start(start_at_ns) do
    start_at_ns
    |> Kernel.-(System.os_time(:nanosecond) + @start_spin_ns)
    |> div(1_000_000)
    |> max(0)
    |> Process.sleep()

    spin_until(start_at_ns)
  end

  defp spin_until(start_at_ns) do
    if System.os_time(:nanosecond) < start_at_ns, do: spin_until(start_at_ns), else: :ok
  end

  def ping_stream(pair_index), do: stream_base() + pair_index * 2
  def pong_stream(pair_index), do: stream_base() + pair_index * 2 + 1

  def channel, do: @channel

  def ping_side(client, pair_index) do
    {:ok, pub} = AeronElixir.add_publication(client, @channel, ping_stream(pair_index))
    {:ok, sub} = AeronElixir.add_subscription(client, @channel, pong_stream(pair_index))
    {:ok, handle} = AeronElixir.publication_handle(pub)
    {handle, sub}
  end

  def pong_side(client, pair_index) do
    {:ok, sub} = AeronElixir.add_subscription(client, @channel, ping_stream(pair_index))
    {:ok, pub} = AeronElixir.add_publication(client, @channel, pong_stream(pair_index))
    {:ok, handle} = AeronElixir.publication_handle(pub)
    {handle, sub}
  end

  def await_image(sub, attempts \\ 3_000)
  def await_image(_sub, 0), do: raise("no image appeared on subscription")

  def await_image(sub, attempts) do
    case AeronElixir.subscription_handles(sub) do
      {:ok, [image | _]} ->
        image

      _ ->
        Process.sleep(1)
        await_image(sub, attempts - 1)
    end
  end

  def pong_loop(handle, image), do: pong_loop(handle, image, 0)

  defp pong_loop(handle, image, tick_sum) do
    case Subscriber.next(image) do
      nil ->
        pong_loop(handle, image, tick_sum)

      payload ->
        offer_until_accepted(handle, payload)
        pong_loop(handle, image, tick_sum + tick_value(payload))
    end
  end

  @doc """
  Folds the four tick fields of a payload into a running sum, matching the
  decode every other client performs on the receive side.
  """
  def tick_value(
        <<instrument_id::little-32, bid::little-signed-64, ask::little-signed-64,
          sequence::little-signed-64, _rest::binary>>
      ) do
    bid + instrument_id + ask + sequence
  end

  def tick_value(_payload), do: 0

  def offer_until_accepted(handle, payload) do
    case Publisher.publish(handle, payload, 0) do
      {:ok, _} -> :ok
      {:error, _} -> offer_until_accepted(handle, payload)
    end
  end

  def round_trip(handle, image, payload) do
    offer_until_accepted(handle, payload)
    await_one(image)
  end

  defp await_one(image) do
    case Subscriber.next(image) do
      nil -> await_one(image)
      payload -> tick_value(payload)
    end
  end

  def run_pinger(handle, image, warmup, count) do
    payload = Shared.payload_pool(message_length())

    warmup_sum =
      Enum.reduce(1..warmup, 0, fn sequence, sum ->
        sum + round_trip(handle, image, Shared.message(payload, sequence))
      end)

    samples = new_samples(count)
    await_start(start_at_ns())
    started_at_ns = System.os_time(:nanosecond)
    start_ns = System.monotonic_time(:nanosecond)

    tick_sum =
      Enum.reduce(1..count, warmup_sum, fn index, sum ->
        t0 = System.monotonic_time(:nanosecond)
        value = round_trip(handle, image, Shared.message(payload, warmup + index))
        :atomics.put(samples, index, System.monotonic_time(:nanosecond) - t0)
        sum + value
      end)

    elapsed_ns = System.monotonic_time(:nanosecond) - start_ns
    finished_at_ns = System.os_time(:nanosecond)
    IO.puts(:stderr, "[bench-elixir-rpc] tick_sum=#{tick_sum}")
    stats(read_samples(samples, count), elapsed_ns, {started_at_ns, finished_at_ns})
  end

  @doc """
  Preallocated storage for one round-trip timing per iteration.

  The measured loop writes into a fixed `:atomics` array so the pinger's heap
  does not grow while it is timing itself; accumulating a list of samples
  instead triggers garbage collection inside the measurement and widens the
  upper percentiles by an order of magnitude.
  """
  def new_samples(count), do: :atomics.new(count, signed: false)

  def read_samples(samples, count), do: for(index <- 1..count, do: :atomics.get(samples, index))

  def beam_pinger(ponger, warmup, count) do
    payload = Shared.payload_pool(message_length())

    warmup_sum =
      Enum.reduce(1..warmup, 0, fn sequence, sum ->
        sum + beam_round_trip(ponger, Shared.message(payload, sequence))
      end)

    samples = new_samples(count)
    await_start(start_at_ns())
    started_at_ns = System.os_time(:nanosecond)
    start_ns = System.monotonic_time(:nanosecond)

    tick_sum =
      Enum.reduce(1..count, warmup_sum, fn index, sum ->
        t0 = System.monotonic_time(:nanosecond)
        value = beam_round_trip(ponger, Shared.message(payload, warmup + index))
        :atomics.put(samples, index, System.monotonic_time(:nanosecond) - t0)
        sum + value
      end)

    elapsed_ns = System.monotonic_time(:nanosecond) - start_ns
    finished_at_ns = System.os_time(:nanosecond)
    IO.puts(:stderr, "[bench-beam-rpc] tick_sum=#{tick_sum}")
    stats(read_samples(samples, count), elapsed_ns, {started_at_ns, finished_at_ns})
  end

  defp beam_round_trip(ponger, payload) do
    send(ponger, {:ping, self(), IO.iodata_to_binary(payload)})

    receive do
      {:pong, echo} -> tick_value(echo)
    end
  end

  def beam_ponger, do: beam_ponger(0)

  defp beam_ponger(tick_sum) do
    receive do
      {:ping, from, payload} ->
        send(from, {:pong, payload})
        beam_ponger(tick_sum + tick_value(payload))

      :stop ->
        :ok
    end
  end

  def stats(samples, elapsed_ns, {started_at_ns, finished_at_ns}) do
    sorted = Enum.sort(samples)
    count = length(sorted)
    mean_ns = Enum.sum(sorted) / count

    %{
      samples: count,
      elapsed_ms: elapsed_ns / 1.0e6,
      mean_us: mean_ns / 1000.0,
      p50_us: percentile(sorted, count, 50.0) / 1000.0,
      p90_us: percentile(sorted, count, 90.0) / 1000.0,
      p99_us: percentile(sorted, count, 99.0) / 1000.0,
      p999_us: percentile(sorted, count, 99.9) / 1000.0,
      max_us: List.last(sorted) / 1000.0,
      round_trips_per_sec: count * 1.0e9 / elapsed_ns,
      started_at_ns: started_at_ns,
      finished_at_ns: finished_at_ns
    }
  end

  defp percentile(sorted, count, pct) do
    index = min(max(trunc(pct / 100.0 * count), 0), count - 1)
    Enum.at(sorted, index)
  end

  def print_json(client, extra, stats) do
    fields =
      [{"client", client}, {"scenario", "rpc"}] ++
        extra ++
        [
          {"message_length", message_length()},
          {"samples", stats.samples},
          {"elapsed_ms", stats.elapsed_ms},
          {"mean_us", stats.mean_us},
          {"p50_us", stats.p50_us},
          {"p90_us", stats.p90_us},
          {"p99_us", stats.p99_us},
          {"p999_us", stats.p999_us},
          {"max_us", stats.max_us},
          {"round_trips_per_sec", stats.round_trips_per_sec},
          {"started_at_ns", stats.started_at_ns},
          {"finished_at_ns", stats.finished_at_ns}
        ]

    body =
      Enum.map_join(fields, ",", fn
        {key, value} when is_binary(value) -> "\"#{key}\":\"#{value}\""
        {key, value} -> "\"#{key}\":#{value}"
      end)

    IO.puts("{" <> body <> "}")
  end
end
