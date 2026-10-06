using System.Diagnostics;
using Adaptive.Aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;

namespace AeronBench;

public static class Kit
{
    public const int TickLength = 28;

    private static readonly double NanosPerTick = 1_000_000_000.0 / Stopwatch.Frequency;

    public static string AeronDir => Env("AERON_DIR", "/tmp/ae_drv");

    public static string TermLength => Env("BENCH_TERM_LENGTH", "16777216");

    public static string Env(string name, string fallback)
    {
        var value = Environment.GetEnvironmentVariable(name);
        return string.IsNullOrEmpty(value) ? fallback : value;
    }

    public static int EnvInt(string name, int fallback) =>
        int.TryParse(Environment.GetEnvironmentVariable(name), out var value) ? value : fallback;

    public static long EnvLong(string name, long fallback) =>
        long.TryParse(Environment.GetEnvironmentVariable(name), out var value) ? value : fallback;

    public static string Arg(string[] args, string name, string fallback)
    {
        var prefix = "--" + name + "=";
        var match = args.FirstOrDefault(arg => arg.StartsWith(prefix, StringComparison.Ordinal));
        return match is null ? fallback : match[prefix.Length..];
    }

    public static Aeron Connect() => Aeron.Connect(new Aeron.Context().AeronDirectoryName(AeronDir));

    public static long NowNs() => (long)(Stopwatch.GetTimestamp() * NanosPerTick);

    public static void WriteTick(UnsafeBuffer buffer, int offset, int length, long sequence)
    {
        if (length < TickLength)
        {
            return;
        }

        var bid = 100_000 + sequence % 1000;
        buffer.PutInt(offset, (int)(sequence % 100));
        buffer.PutLong(offset + 4, bid);
        buffer.PutLong(offset + 12, bid + 10);
        buffer.PutLong(offset + 20, sequence);
    }

    public static long ReadTick(IDirectBuffer buffer, int offset, int length)
    {
        if (length < TickLength)
        {
            return 0;
        }

        return buffer.GetLong(offset + 4)
            + (uint)buffer.GetInt(offset)
            + buffer.GetLong(offset + 12)
            + buffer.GetLong(offset + 20);
    }

    public static double PercentileMicros(long[] sorted, int count, double pct)
    {
        if (count == 0)
        {
            return 0;
        }

        var index = Math.Clamp((int)(pct / 100.0 * count), 0, count - 1);
        return sorted[index] / 1000.0;
    }

    public static double MeanMicros(long[] samples, int count)
    {
        if (count == 0)
        {
            return 0;
        }

        double total = 0;
        for (var index = 0; index < count; index++)
        {
            total += samples[index];
        }

        return total / count / 1000.0;
    }

    public static void Fail(string message)
    {
        Console.Error.WriteLine(message);
        Environment.Exit(1);
    }
}

public sealed class PayloadPool
{
    public const int Entries = 4096;

    private const ulong Seed = 0x5EEDAE20UL;

    public PayloadPool(int size)
    {
        Size = size;
        var total = Entries * size;
        Buffer = new UnsafeBuffer(BufferUtil.AllocateDirectAligned(total, 64));

        var state = Seed;
        for (var offset = 0; offset < total; offset += 8)
        {
            state += 0x9E3779B97F4A7C15UL;
            var mixed = state;
            mixed = (mixed ^ (mixed >> 30)) * 0xBF58476D1CE4E5B9UL;
            mixed = (mixed ^ (mixed >> 27)) * 0x94D049BB133111EBUL;
            mixed ^= mixed >> 31;
            for (var index = 0; index < 8 && offset + index < total; index++)
            {
                Buffer.PutByte(offset + index, (byte)(mixed >> (8 * index)));
            }
        }
    }

    public UnsafeBuffer Buffer { get; }

    public int Size { get; }

    public int Offset(long sequence) => (int)(sequence % Entries) * Size;

    public int Message(long sequence)
    {
        var offset = Offset(sequence);
        Kit.WriteTick(Buffer, offset, Size, sequence);
        return offset;
    }
}
