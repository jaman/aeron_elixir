using System.Globalization;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using AeronBench;

var workers = int.Parse(Kit.Arg(args, "workers", "4"), CultureInfo.InvariantCulture);
var size = int.Parse(Kit.Arg(args, "size", "256"), CultureInfo.InvariantCulture);
var timeSeconds = int.Parse(Kit.Arg(args, "time", "5"), CultureInfo.InvariantCulture);
var warmupSeconds = int.Parse(Kit.Arg(args, "warmup", "2"), CultureInfo.InvariantCulture);

Console.Error.WriteLine($"[bench-dotnet-mp] workers={workers} size={size} time={timeSeconds}s warmup={warmupSeconds}s dir={Kit.AeronDir}");

using var aeron = Kit.Connect();
var results = new Worker.Result[workers];
var threads = Enumerable.Range(0, workers)
    .Select(index => new Thread(() => results[index] = Worker.Run(aeron, index, size, timeSeconds, warmupSeconds)))
    .ToArray();

foreach (var thread in threads)
{
    thread.Start();
}

foreach (var thread in threads)
{
    thread.Join();
}

if (results.Any(result => result.Failure is not null))
{
    Kit.Fail("[bench-dotnet-mp] " + string.Join("; ", results.Where(r => r.Failure is not null).Select(r => r.Failure)));
}

var sent = results.Sum(result => result.Sent);
var received = results.Sum(result => result.Received);
var slowest = results.Max(result => result.ElapsedNs);
var opsPerSec = sent * 1e9 / slowest;

Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
    $"{{\"client\":\"dotnet\",\"scenario\":\"throughput\",\"payload_size\":{size},\"workers\":{workers}," +
    $"\"samples\":{sent},\"received\":{received},\"ops_per_sec\":{opsPerSec:F3},\"bytes_per_sec\":{opsPerSec * size:F3}," +
    $"\"elapsed_ms\":{slowest / 1e6:F3}}}"));

internal static class Worker
{
    public readonly record struct Result(long Sent, long Received, long ElapsedNs, string? Failure);

    public static Result Run(Aeron aeron, int index, int size, int timeSeconds, int warmupSeconds)
    {
        var channel = $"aeron:ipc?alias=dotnet-mp-{index}-{Kit.NowNs()}|term-length={Kit.TermLength}";
        var streamId = (int)((Kit.NowNs() + index) & 0x7FFFFFFF);

        using var publication = aeron.AddPublication(channel, streamId);
        using var subscription = aeron.AddSubscription(channel, streamId);

        var connectDeadline = Kit.NowNs() + 10_000_000_000L;
        while (!(publication.IsConnected && subscription.IsConnected))
        {
            if (Kit.NowNs() > connectDeadline)
            {
                return new Result(0, 0, 0, $"worker {index} connection timeout");
            }

            Thread.Yield();
        }

        var pool = new PayloadPool(size);
        long received = 0;
        long tickSum = 0;
        FragmentHandler handler = (buffer, offset, length, _) =>
        {
            received++;
            tickSum += Kit.ReadTick(buffer, offset, length);
        };

        bool Offer(long sequence, bool drain)
        {
            var offset = pool.Message(sequence);
            while (true)
            {
                var result = publication.Offer(pool.Buffer, offset, size);
                if (result > 0)
                {
                    return true;
                }

                if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED)
                {
                    return false;
                }

                if (drain)
                {
                    subscription.Poll(handler, 1024);
                }
            }
        }

        long sequence = 0;
        var warmupDeadline = Kit.NowNs() + warmupSeconds * 1_000_000_000L;
        while (Kit.NowNs() < warmupDeadline)
        {
            if (!Offer(sequence++, false))
            {
                return new Result(0, 0, 0, $"worker {index} publication closed");
            }

            subscription.Poll(handler, 1024);
        }

        while (subscription.Poll(handler, 1024) > 0)
        {
        }

        received = 0;
        tickSum = 0;
        sequence = 0;
        long sent = 0;
        var start = Kit.NowNs();
        var runDeadline = start + timeSeconds * 1_000_000_000L;
        while (Kit.NowNs() < runDeadline)
        {
            for (var count = 0; count < 1000; count++)
            {
                if (!Offer(sequence++, true))
                {
                    return new Result(0, 0, 0, $"worker {index} publication closed");
                }

                sent++;
            }

            subscription.Poll(handler, 1024);
        }

        var elapsed = Kit.NowNs() - start;
        while (received < sent && subscription.Poll(handler, 1024) > 0)
        {
        }

        Console.Error.WriteLine($"[bench-dotnet-mp] worker {index} tick_sum={tickSum}");
        return new Result(sent, received, elapsed, null);
    }
}
