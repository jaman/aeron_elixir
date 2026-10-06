using System.Globalization;
using System.Runtime.InteropServices;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using AeronBench;

var mode = Kit.Arg(args, "mode", "ping");
var streamBase = Kit.EnvInt("RPC_STREAM_BASE", 7100);
var pairIndex = Kit.EnvInt("RPC_PAIR_INDEX", 0);
var pingStream = streamBase + pairIndex * 2;
var pongStream = pingStream + 1;
var length = Kit.EnvInt("RPC_MESSAGE_LENGTH", 32);
var warmup = Kit.EnvInt("RPC_WARMUP_MESSAGES", 100_000);
var messages = Kit.EnvInt("RPC_MESSAGES", 1_000_000);
var startAtNs = Kit.EnvLong("RPC_START_AT_NS", 0);

Console.Error.WriteLine($"[bench-dotnet-rpc] mode={mode} ping={pingStream} pong={pongStream} length={length} dir={Kit.AeronDir}");

using var aeron = Kit.Connect();

switch (mode)
{
    case "ping":
        RpcRuns.Ping(aeron, pingStream, pongStream, length, warmup, messages, startAtNs);
        break;
    case "pong":
        RpcRuns.Pong(aeron, pingStream, pongStream);
        break;
    default:
        Kit.Fail($"[bench-dotnet-rpc] unknown mode {mode}");
        break;
}

internal static class RpcRuns
{
    public static void Ping(Aeron aeron, int pingStream, int pongStream, int length, int warmup, int messages, long startAtNs)
    {
        using var subscription = aeron.AddSubscription("aeron:ipc", pongStream);
        using var publication = aeron.AddPublication("aeron:ipc", pingStream);
        AwaitConnected(publication, subscription);

        var pool = new PayloadPool(length);
        long received = 0;
        long tickSum = 0;
        FragmentHandler handler = (buffer, offset, fragmentLength, _) =>
        {
            received++;
            tickSum += Kit.ReadTick(buffer, offset, fragmentLength);
        };

        long sequence = 0;

        void RoundTrip()
        {
            var offset = pool.Message(sequence++);
            while (publication.Offer(pool.Buffer, offset, length) < 0)
            {
            }

            received = 0;
            while (received == 0)
            {
                subscription.Poll(handler, 1);
            }
        }

        for (var index = 0; index < warmup; index++)
        {
            RoundTrip();
        }

        var samples = new long[messages];
        var sleepNs = startAtNs - RealtimeClock.NowNs() - 2_000_000;
        if (sleepNs > 0)
        {
            Thread.Sleep(TimeSpan.FromTicks(sleepNs / 100));
        }

        while (RealtimeClock.NowNs() < startAtNs)
        {
        }

        var startedAtNs = RealtimeClock.NowNs();
        var start = Kit.NowNs();
        for (var index = 0; index < messages; index++)
        {
            var t0 = Kit.NowNs();
            RoundTrip();
            samples[index] = Kit.NowNs() - t0;
        }

        var elapsed = Kit.NowNs() - start;
        var finishedAtNs = RealtimeClock.NowNs();
        var mean = Kit.MeanMicros(samples, messages);
        Array.Sort(samples);

        Console.Error.WriteLine($"[bench-dotnet-rpc] tick_sum={tickSum}");
        Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"{{\"client\":\"dotnet\",\"scenario\":\"rpc\",\"pairs\":1,\"message_length\":{length},\"samples\":{messages}," +
            $"\"elapsed_ms\":{elapsed / 1e6:F3},\"mean_us\":{mean:F4},\"p50_us\":{Kit.PercentileMicros(samples, messages, 50):F4}," +
            $"\"p90_us\":{Kit.PercentileMicros(samples, messages, 90):F4},\"p99_us\":{Kit.PercentileMicros(samples, messages, 99):F4}," +
            $"\"p999_us\":{Kit.PercentileMicros(samples, messages, 99.9):F4},\"max_us\":{samples[messages - 1] / 1000.0:F4}," +
            $"\"round_trips_per_sec\":{messages * 1e9 / elapsed:F3},\"started_at_ns\":{startedAtNs},\"finished_at_ns\":{finishedAtNs}}}"));
    }

    public static void Pong(Aeron aeron, int pingStream, int pongStream)
    {
        var running = true;
        using var terminate = PosixSignalRegistration.Create(PosixSignal.SIGTERM, context =>
        {
            context.Cancel = true;
            Volatile.Write(ref running, false);
        });

        using var subscription = aeron.AddSubscription("aeron:ipc", pingStream);
        using var publication = aeron.AddPublication("aeron:ipc", pongStream);
        Console.WriteLine("READY");
        Console.Out.Flush();
        AwaitConnected(publication, subscription);

        long received = 0;
        long tickSum = 0;
        FragmentHandler handler = (buffer, offset, fragmentLength, _) =>
        {
            received++;
            tickSum += Kit.ReadTick(buffer, offset, fragmentLength);
            while (Volatile.Read(ref running))
            {
                var result = publication.Offer(buffer, offset, fragmentLength);
                if (result > 0)
                {
                    return;
                }

                if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED)
                {
                    Console.Error.WriteLine($"[bench-dotnet-rpc] echo failed: {result}");
                    Volatile.Write(ref running, false);
                    return;
                }
            }
        };

        while (Volatile.Read(ref running))
        {
            subscription.Poll(handler, 1);
        }

        Console.Error.WriteLine($"[bench-dotnet-rpc] pong tick_sum={tickSum} received={received}");
    }

    private static void AwaitConnected(Publication publication, Subscription subscription)
    {
        var deadline = Kit.NowNs() + 30_000_000_000L;
        while (!(publication.IsConnected && subscription.IsConnected))
        {
            if (Kit.NowNs() > deadline)
            {
                Kit.Fail("[bench-dotnet-rpc] connection timeout");
            }

            Thread.Yield();
        }
    }
}
