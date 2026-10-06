defmodule AeronElixir.Bench.Shared do
  @moduledoc false

  import Bitwise

  alias AeronElixir.LogBuffer.Publisher
  alias AeronElixir.LogBuffer.Subscriber
  alias AeronElixir.NIF

  @pool_entries 4096
  @pool_seed 0x5EEDAE20
  @mask64 0xFFFFFFFFFFFFFFFF

  @aeron_directory System.get_env("AERON_DIR", "/tmp/ae_drv")

  @payload_sizes Enum.map(
                   System.get_env("BENCH_PAYLOAD_SIZES", "32,256,1024") |> String.split(","),
                   &String.to_integer/1
                 )
  @warmup_seconds String.to_integer(System.get_env("BENCH_WARMUP_S", "3"))
  @time_seconds String.to_integer(System.get_env("BENCH_TIME_S", "10"))
  @term_length String.to_integer(System.get_env("BENCH_TERM_LENGTH", "16777216"))

  def aeron_directory, do: @aeron_directory

  def payload_sizes, do: @payload_sizes

  def warmup_seconds, do: @warmup_seconds

  def time_seconds, do: @time_seconds

  def term_length, do: @term_length

  def publish_mode, do: System.get_env("BENCH_PUBLISH_MODE", "public")

  def raw?, do: publish_mode() == "direct"

  def deliver?, do: System.get_env("BENCH_DELIVER", "1") == "1"

  def client_label, do: System.get_env("BENCH_CLIENT_LABEL", "elixir")

  @doc """
  Returns a `payload -> {:ok, position} | {:error, reason}` publish function.

  In raw mode it appends directly through `LogBuffer.Publisher`, bypassing the
  conductor `GenServer`; otherwise it uses the public `AeronElixir.publish/2` API.
  """
  def make_publish(pub) do
    if raw?() do
      {:ok, runtime} = AeronElixir.publication_handle(pub)
      fn payload -> raw_publish(runtime, payload) end
    else
      fn payload -> AeronElixir.publish(pub, payload) end
    end
  end

  @doc """
  Returns a `(pool, count) -> :ok` function that publishes the next `count`
  messages from `pool`, each a distinct tick stamped into its pool entry, and
  drains the subscriber whenever the publication back-pressures.

  `direct` mode publishes one message per call through the publication handle;
  `list` mode hands each batch to `Publisher.publish_list/3` in one native call;
  any other mode publishes one message per call through `AeronElixir.publish/2`.
  """
  def make_publish_batch(pub, sub) do
    case publish_mode() do
      "direct" ->
        {:ok, runtime} = AeronElixir.publication_handle(pub)
        images = raw_images(sub)
        fn payload, count -> tick_offer_n(&raw_publish(runtime, &1), fn -> raw_drain(images) end, payload, count) end

      "list" ->
        {:ok, runtime} = AeronElixir.publication_handle(pub)
        images = raw_images(sub)
        fn payload, count -> list_offer_n(runtime, images, payload, count) end

      _ ->
        publish = make_publish(pub)
        drain = make_drain_all(sub)
        fn payload, count -> tick_offer_n(publish, drain, payload, count) end
    end
  end

  defp list_offer_n(runtime, images, payload, count) do
    sequence = Process.get(:bench_sequence, 0)
    list_offer_loop(runtime, images, payload, sequence, count)
    Process.put(:bench_sequence, sequence + count)
    :ok
  end

  defp list_offer_loop(_runtime, _images, _payload, _sequence, 0), do: :ok

  defp list_offer_loop(runtime, images, payload, sequence, remaining) do
    payloads = for offset <- 0..(remaining - 1), do: message(payload, sequence + offset)
    published = Publisher.publish_list(runtime, payloads, 0)

    if published < remaining do
      raw_drain(images)
      list_offer_loop(runtime, images, payload, sequence + published, remaining - published)
    else
      :ok
    end
  end

  @doc """
  Encodes the benchmark tick the C, Java and Python benches write into every
  message as iodata: a 28-byte header of instrument id (u32), bid (i64), ask (i64)
  and sequence (i64), followed by the payload's remaining bytes without copying them.
  """
  def tick_payload(payload, sequence) when byte_size(payload) >= 28 do
    instrument_id = rem(sequence, 100)
    bid = 100_000 + rem(sequence, 1000)
    ask = bid + 10

    [
      <<instrument_id::little-32, bid::little-signed-64, ask::little-signed-64,
        sequence::little-signed-64>>,
      binary_part(payload, 28, byte_size(payload) - 28)
    ]
  end

  def tick_payload(payload, _sequence), do: payload

  @doc """
  Decodes the tick fields from a received payload and folds them into `sum`,
  mirroring the native benches' receive-side work.
  """
  def tick_sum(
        <<instrument_id::little-32, bid::little-signed-64, ask::little-signed-64,
          sequence::little-signed-64, _::binary>>,
        sum
      ) do
    sum + bid + instrument_id + ask + sequence
  end

  def tick_sum(_payload, sum), do: sum

  defp tick_offer_n(publish, drain, payload, count) do
    sequence = Process.get(:bench_sequence, 0)
    tick_offer_loop(publish, drain, payload, sequence, count)
    Process.put(:bench_sequence, sequence + count)
    :ok
  end

  defp tick_offer_loop(_publish, _drain, _payload, _sequence, 0), do: :ok

  defp tick_offer_loop(publish, drain, payload, sequence, remaining) do
    case publish.(message(payload, sequence)) do
      {:ok, _} ->
        tick_offer_loop(publish, drain, payload, sequence + 1, remaining - 1)

      {:error, _} ->
        drain.()
        tick_offer_loop(publish, drain, payload, sequence, remaining)
    end
  end

  defp raw_publish(runtime, payload) do
    case Publisher.publish(runtime, payload, 0) do
      {:ok, position} -> {:ok, position}
      {:error, :max_position_exceeded} = error -> error
      {:error, _retryable} -> raw_publish(runtime, payload)
    end
  end

  defp raw_images(sub) do
    {:ok, images} = AeronElixir.subscription_handles(sub)
    images
  end

  defp raw_drain(images) do
    if deliver?() do
      Enum.reduce(images, 0, fn image, acc ->
        {count, payloads} = Subscriber.collect(image, 1024)
        Process.put(:bench_tick_sum, Enum.reduce(payloads, Process.get(:bench_tick_sum, 0), &tick_sum/2))
        acc + count
      end)
    else
      Enum.reduce(images, 0, fn image, acc ->
        acc + NIF.poll_count(image.geometry, 1024)
      end)
    end
  end

  def unique_channel(prefix) do
    "aeron:ipc?alias=#{prefix}-#{unique_token()}|term-length=#{@term_length}"
  end

  @doc """
  A stream id that is unique across separate OS processes.

  Each benchmark cell runs in its own `mix run` (a fresh BEAM), so
  `:erlang.unique_integer/1` resets and would otherwise produce the same stream id
  in consecutive cells. Combined with the driver's resource linger window, that
  caused a new cell to rendezvous with the previous cell's lingering image. Hashing
  the OS pid and wall-clock time yields a process-unique positive int32.
  """
  def unique_stream_id do
    :erlang.phash2({System.pid(), System.system_time(), :erlang.unique_integer()}, 0x7FFFFFFF) + 1
  end

  defp unique_token do
    "#{System.pid()}-#{:erlang.unique_integer([:positive])}"
  end

@doc """
Builds the payload pool every benchmark client uses: 4096 payloads
of `size` bytes, cut in order from one byte stream made of splitmix64 outputs
(seed 0x5EEDAE20) written little-endian. The C, C++, Java, Python and Go
benches build the same bytes. Message `i` is `message(pool, i)`.
"""
def payload_pool(size) do
  total = @pool_entries * size

  stream =
    @pool_seed
    |> splitmix_words(div(total + 7, 8), [])
    |> IO.iodata_to_binary()
    |> binary_part(0, total)

  0..(@pool_entries - 1)
  |> Enum.map(&binary_part(stream, &1 * size, size))
  |> List.to_tuple()
end

@doc """
Returns message `sequence`: pool entry `rem(sequence, 4096)` with
the tick for `sequence` stamped over its first 28 bytes, as iodata.
"""
def message(pool, sequence), do: pool |> elem(rem(sequence, @pool_entries)) |> tick_payload(sequence)

defp splitmix_words(_state, 0, acc), do: Enum.reverse(acc)

defp splitmix_words(state, remaining, acc) do
  state = band(state + 0x9E3779B97F4A7C15, @mask64)
  mixed = band(bxor(state, state >>> 30) * 0xBF58476D1CE4E5B9, @mask64)
  mixed = band(bxor(mixed, mixed >>> 27) * 0x94D049BB133111EB, @mask64)
  splitmix_words(state, remaining - 1, [<<bxor(mixed, mixed >>> 31)::little-64>> | acc])
end

  def start_client do
    {:ok, pid} = AeronElixir.start_link(aeron_directory: @aeron_directory)
    pid
  end

  def setup_pub_sub(client, channel, stream_id) do
    {:ok, pub} = AeronElixir.add_publication(client, channel, stream_id)
    {:ok, sub} = AeronElixir.add_subscription(client, channel, stream_id)
    {pub, sub}
  end

  def await_first_message(pub, sub, payload, attempts \\ 500)
  def await_first_message(_pub, _sub, _payload, 0), do: raise("subscription never received a message within warmup window")

  def await_first_message(pub, sub, pool, attempts) when attempts > 0 do
    {:ok, _} = AeronElixir.publish(pub, message(pool, 0))
    Process.sleep(2)

    case drain_all(sub, 50) do
      0 -> await_first_message(pub, sub, pool, attempts - 1)
      _ -> :ok
    end
  end

  @doc """
  Returns a zero-arity function that drains all available messages (throughput loop).
  """
  def make_drain_all(sub) do
    case publish_mode() do
      "public" ->
        fn -> drain_all(sub) end

      _ ->
        images = raw_images(sub)
        fn -> raw_drain_all(images, 0) end
    end
  end

  def tick_sum_total, do: Process.get(:bench_tick_sum, 0)

  defp raw_drain_all(images, count) do
    case raw_drain(images) do
      0 -> count
      n -> raw_drain_all(images, count + n)
    end
  end

  def drain_all(sub, max_attempts \\ 1_000) do
    drain_all_loop(sub, 0, max_attempts)
  end

  defp drain_all_loop(_sub, count, 0), do: count

  defp drain_all_loop(sub, count, attempts_left) do
      n =
        AeronElixir.poll(sub, 64, fn payload, _header ->
          Process.put(:bench_tick_sum, tick_sum(payload, Process.get(:bench_tick_sum, 0)))
        end)

      if n == 0 do
        count
      else
        drain_all_loop(sub, count + n, attempts_left - 1)
      end
  end
end
