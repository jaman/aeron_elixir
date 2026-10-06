defmodule Harness.Hist do
  @moduledoc false

  @buckets 100_000
  @bucket_ns 100

  def new, do: :atomics.new(@buckets + 2, signed: false)

  def record(hist, latency_ns) when latency_ns < 0 do
    :atomics.add(hist, @buckets + 2, 1)
    hist
  end

  def record(hist, latency_ns) do
    bucket = min(div(latency_ns, @bucket_ns), @buckets - 1)
    :atomics.add(hist, bucket + 1, 1)
    bump_max(hist, latency_ns)
    hist
  end

  defp bump_max(hist, latency_ns) do
    current = :atomics.get(hist, @buckets + 1)
    store_max(hist, current, latency_ns)
  end

  defp store_max(_hist, current, latency_ns) when current >= latency_ns, do: :ok
  defp store_max(hist, _current, latency_ns), do: :atomics.put(hist, @buckets + 1, latency_ns)

  def negatives(hist), do: :atomics.get(hist, @buckets + 2)

  def max_ns(hist), do: :atomics.get(hist, @buckets + 1)

  def total(hist) do
    Enum.reduce(1..@buckets, 0, fn index, acc -> acc + :atomics.get(hist, index) end)
  end

  def percentiles(hist, fractions) do
    counts = for index <- 1..@buckets, do: :atomics.get(hist, index)
    total = Enum.sum(counts)
    Enum.map(fractions, fn fraction -> percentile(counts, total, fraction) end)
  end

  defp percentile(_counts, 0, _fraction), do: 0.0

  defp percentile(counts, total, fraction) do
    target = Float.ceil(total * fraction) |> trunc() |> max(1)

    counts
    |> Enum.with_index()
    |> Enum.reduce_while(0, fn {count, index}, seen ->
      seen = seen + count
      if seen >= target, do: {:halt, (index * @bucket_ns + div(@bucket_ns, 2)) / 1000}, else: {:cont, seen}
    end)
    |> normalise()
  end

  defp normalise(value) when is_float(value), do: Float.round(value, 3)
  defp normalise(_), do: 0.0

  def mean_us(hist) do
    {sum, count} =
      Enum.reduce(1..@buckets, {0, 0}, fn index, {sum, count} ->
        n = :atomics.get(hist, index)
        {sum + n * ((index - 1) * @bucket_ns + div(@bucket_ns, 2)), count + n}
      end)

    mean(sum, count)
  end

  defp mean(_sum, 0), do: 0.0
  defp mean(sum, count), do: Float.round(sum / count / 1000, 3)
end
