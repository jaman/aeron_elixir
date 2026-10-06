require Logger

Logger.configure(level: :warning)

Code.require_file("hist.exs", __DIR__)

defmodule Harness.Consumer do
  @moduledoc false

  alias AeronElixir.LogBuffer.Subscriber
  alias Harness.Hist

  @poll_limit 1024
  @idle_exit_ms 750

  def run(opts) do
    {:ok, client} = AeronElixir.start_link(aeron_directory: opts.aeron_dir)
    {:ok, subscription} = AeronElixir.add_subscription(client, opts.channel, opts.stream)

    IO.puts("READY")
    IO.puts(:stderr, "[consumer-#{opts.label}] subscribed, awaiting image")

    :ok = AeronElixir.await_connected(subscription, 60_000)
    image = hd(AeronElixir.images(subscription))
    IO.puts(:stderr, "[consumer-#{opts.label}] image attached")

    hist = Hist.new()
    deadline = :os.system_time(:nanosecond) + opts.duration_s * 1_000_000_000

    state = %{
      consumed: 0,
      first_sequence: -1,
      last_sequence: -1,
      gaps: 0,
      price_sum: 0,
      started_at: 0
    }

    state = drain(opts, image, subscription, hist, state, deadline, :os.system_time(:nanosecond))

    report(opts, hist, state)
  end

  defp drain(opts, image, subscription, hist, state, deadline, last_message_at) do
    now = :os.system_time(:nanosecond)

    cond do
      now >= deadline ->
        state

      state.consumed > 0 and now - last_message_at > @idle_exit_ms * 1_000_000 ->
        state

      true ->
        {count, state} = consume(opts, image, subscription, hist, state)
        last_message_at = advance(count, now, last_message_at)
        drain(opts, image, subscription, hist, state, deadline, last_message_at)
    end
  end

  defp advance(0, _now, last_message_at), do: last_message_at
  defp advance(_count, now, _last_message_at), do: now

  defp consume(%{mode: :direct} = _opts, image, _subscription, hist, state) do
    {count, payloads} = Subscriber.collect(image, @poll_limit)
    {count, Enum.reduce(payloads, state, &record(&1, &2, hist))}
  end

  defp consume(%{mode: :public}, _image, subscription, hist, state) do
    Process.put(:harness_state, state)

    count =
      AeronElixir.poll(subscription, @poll_limit, fn payload, _header ->
        Process.put(:harness_state, record(payload, Process.get(:harness_state), hist))
      end)

    {count, Process.get(:harness_state)}
  end

  defp record(
         <<instrument_id::little-32, bid::little-signed-64, ask::little-signed-64,
           sequence::little-signed-64, published_at::little-signed-64, _rest::binary>>,
         state,
         hist
       ) do
    Hist.record(hist, :os.system_time(:nanosecond) - published_at)

    %{
      state
      | consumed: state.consumed + 1,
        first_sequence: first_of(state.first_sequence, sequence),
        last_sequence: sequence,
        gaps: state.gaps + gap(state.last_sequence, sequence),
        price_sum: state.price_sum + bid + ask + instrument_id
    }
  end

  defp record(_payload, state, _hist), do: state

  defp first_of(-1, sequence), do: sequence
  defp first_of(existing, _sequence), do: existing

  defp gap(-1, _sequence), do: 0
  defp gap(previous, sequence) when sequence == previous + 1, do: 0
  defp gap(_previous, _sequence), do: 1

  defp report(opts, hist, state) do
    [p50, p90, p99, p999] = Hist.percentiles(hist, [0.5, 0.9, 0.99, 0.999])
    span = state.last_sequence - state.first_sequence + 1

    IO.puts(:stderr, "[consumer-#{opts.label}] price_sum=#{state.price_sum}")

    IO.puts(
      ~s({"role":"consumer","client":"#{opts.label}","consumed":#{state.consumed},) <>
        ~s("first_sequence":#{state.first_sequence},"last_sequence":#{state.last_sequence},) <>
        ~s("sequence_span":#{span},"gaps":#{state.gaps},) <>
        ~s("p50_us":#{p50},"p90_us":#{p90},"p99_us":#{p99},"p999_us":#{p999},) <>
        ~s("max_us":#{Float.round(Hist.max_ns(hist) / 1000, 3)},) <>
        ~s("mean_us":#{Hist.mean_us(hist)},"negative_latencies":#{Hist.negatives(hist)}})
    )
  end
end

mode =
  case System.get_env("HARNESS_MODE", "direct") do
    "public" -> :public
    _ -> :direct
  end

opts = %{
  aeron_dir: System.get_env("AERON_DIR", "/tmp/ae_drv"),
  channel: System.get_env("HARNESS_CHANNEL", "aeron:ipc?alias=prices|term-length=67108864"),
  stream: String.to_integer(System.get_env("HARNESS_STREAM", "9001")),
  duration_s: String.to_integer(System.get_env("HARNESS_DURATION_S", "30")) + 10,
  mode: mode,
  label: System.get_env("HARNESS_LABEL", "elixir")
}

{:ok, _} = Application.ensure_all_started(:aeron_elixir)
Harness.Consumer.run(opts)
