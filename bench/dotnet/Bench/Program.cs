using System.Globalization;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using AeronBench;

var mode = Kit.Arg(args, "mode", "latency");
var size = int.Parse(Kit.Arg(args, "size", "32"), CultureInfo.InvariantCulture);
var timeSeconds = int.Parse(Kit.Arg(args, "time", "10"), CultureInfo.InvariantCulture);
var warmupSeconds = int.Parse(Kit.Arg(args, "warmup", "3"), CultureInfo.InvariantCulture);

Console.Error.WriteLine($"[bench-dotnet] mode={mode} size={size} time={timeSeconds}s warmup={warmupSeconds}s dir={Kit.AeronDir}");

using var aeron = Kit.Connect();
var channel = $"aeron:ipc?alias=dotnet-bench-{Kit.NowNs()}|term-length={Kit.TermLength}";
var streamId = (int)(Kit.NowNs() & 0x7FFFFFFF);

using var publication = aeron.AddPublication(channel, streamId);
using var subscription = aeron.AddSubscription(channel, streamId);

var connectDeadline = Kit.NowNs() + 10_000_000_000L;
while (!(publication.IsConnected && subscription.IsConnected))
{
    if (Kit.NowNs() > connectDeadline)
    {
        Kit.Fail("[bench-dotnet] connection timeout");
    }

    Thread.Yield();
}

var pool = new PayloadPool(size);

switch (mode)
{
    case "latency":
        Runs.Latency(publication, subscription, pool, size, timeSeconds, warmupSeconds);
        break;
    case "throughput":
        Runs.Throughput(publication, subscription, pool, size, timeSeconds, warmupSeconds);
        break;
    default:
        Kit.Fail($"[bench-dotnet] unknown mode {mode}");
        break;
}

internal static class Runs
{
    public static void Latency(Publication publication, Subscription subscription, PayloadPool pool, int size, int timeSeconds, int warmupSeconds)
    {
        long count = 0;
        FragmentHandler handler = (_, _, _, _) => count++;
        long sequence = 0;

        void RoundTrip()
        {
            var offset = pool.Message(sequence++);
            OfferUntilAccepted(publication, pool, offset, size);
            count = 0;
            while (count == 0)
            {
                subscription.Poll(handler, 1);
            }
        }

        var warmupDeadline = Kit.NowNs() + warmupSeconds * 1_000_000_000L;
        while (Kit.NowNs() < warmupDeadline)
        {
            RoundTrip();
        }

        var samples = new long[timeSeconds * 10_000_000];
        var sampleCount = 0;
        var start = Kit.NowNs();
        var runDeadline = start + timeSeconds * 1_000_000_000L;
        while (sampleCount < samples.Length && Kit.NowNs() < runDeadline)
        {
            var t0 = Kit.NowNs();
            RoundTrip();
            samples[sampleCount++] = Kit.NowNs() - t0;
        }

        var elapsed = Kit.NowNs() - start;
        var mean = Kit.MeanMicros(samples, sampleCount);
        Array.Sort(samples, 0, sampleCount);

        Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"{{\"client\":\"dotnet\",\"scenario\":\"latency\",\"payload_size\":{size},\"samples\":{sampleCount}," +
            $"\"ops_per_sec\":{sampleCount * 1e9 / elapsed:F3},\"elapsed_ms\":{elapsed / 1e6:F3},\"mean_us\":{mean:F4}," +
            $"\"median_us\":{Kit.PercentileMicros(samples, sampleCount, 50):F4},\"p99_us\":{Kit.PercentileMicros(samples, sampleCount, 99):F4}," +
            $"\"p999_us\":{Kit.PercentileMicros(samples, sampleCount, 99.9):F4},\"min_us\":{Kit.PercentileMicros(samples, sampleCount, 0):F4}," +
            $"\"max_us\":{samples[sampleCount - 1] / 1000.0:F4}}}"));
    }

    public static void Throughput(Publication publication, Subscription subscription, PayloadPool pool, int size, int timeSeconds, int warmupSeconds)
    {
        long received = 0;
        long tickSum = 0;
        FragmentHandler handler = (buffer, offset, length, _) =>
        {
            received++;
            tickSum += Kit.ReadTick(buffer, offset, length);
        };

        long sequence = 0;
        var warmupDeadline = Kit.NowNs() + warmupSeconds * 1_000_000_000L;
        while (Kit.NowNs() < warmupDeadline)
        {
            OfferUntilAccepted(publication, pool, pool.Message(sequence++), size);
            subscription.Poll(handler, 1024);
        }

        received = 0;
        tickSum = 0;
        sequence = 0;
        long sent = 0;
        var start = Kit.NowNs();
        var runDeadline = start + timeSeconds * 1_000_000_000L;
        while (Kit.NowNs() < runDeadline)
        {
            for (var index = 0; index < 1000; index++)
            {
                var offset = pool.Message(sequence++);
                while (true)
                {
                    var result = publication.Offer(pool.Buffer, offset, size);
                    if (result > 0)
                    {
                        break;
                    }

                    FailWhenClosed(result);
                    subscription.Poll(handler, 1024);
                }

                sent++;
            }

            subscription.Poll(handler, 1024);
        }

        var elapsed = Kit.NowNs() - start;
        while (received < sent && subscription.Poll(handler, 1024) > 0)
        {
        }

        Console.Error.WriteLine($"[bench-dotnet] throughput tick_sum={tickSum}");
        var opsPerSec = sent * 1e9 / elapsed;
        Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"{{\"client\":\"dotnet\",\"scenario\":\"throughput\",\"payload_size\":{size},\"samples\":{sent}," +
            $"\"ops_per_sec\":{opsPerSec:F3},\"bytes_per_sec\":{opsPerSec * size:F3},\"received\":{received},\"elapsed_ms\":{elapsed / 1e6:F3}}}"));
    }

    private static void OfferUntilAccepted(Publication publication, PayloadPool pool, int offset, int size)
    {
        while (true)
        {
            var result = publication.Offer(pool.Buffer, offset, size);
            if (result > 0)
            {
                return;
            }

            FailWhenClosed(result);
        }
    }

    private static void FailWhenClosed(long result)
    {
        if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED)
        {
            Kit.Fail($"[bench-dotnet] publication closed: {result}");
        }
    }
}
