using System.Globalization;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using AeronBench;

const int Buckets = 100_000;
const int BucketNs = 100;
const long IdleExitNs = 750_000_000L;

var channel = Kit.Env("HARNESS_CHANNEL", "aeron:ipc?alias=prices|term-length=67108864");
var stream = Kit.EnvInt("HARNESS_STREAM", 9001);
var durationSeconds = Kit.EnvInt("HARNESS_DURATION_S", 30) + 10;
var label = Kit.Env("HARNESS_LABEL", "dotnet");

var counts = new long[Buckets];
long consumed = 0;
long firstSequence = -1;
long lastSequence = -1;
long gaps = 0;
long priceSum = 0;
long negatives = 0;
long maxNs = 0;

void OnTick(IDirectBuffer buffer, int offset, int length, Header header)
{
    if (length < 36)
    {
        return;
    }

    var instrumentId = (uint)buffer.GetInt(offset);
    var bid = buffer.GetLong(offset + 4);
    var ask = buffer.GetLong(offset + 12);
    var sequence = buffer.GetLong(offset + 20);
    var publishedAt = buffer.GetLong(offset + 28);

    var latency = RealtimeClock.NowNs() - publishedAt;
    if (latency < 0)
    {
        negatives++;
    }
    else
    {
        counts[Math.Min(latency / BucketNs, Buckets - 1)]++;
        maxNs = Math.Max(maxNs, latency);
    }

    if (firstSequence < 0)
    {
        firstSequence = sequence;
    }
    else if (sequence != lastSequence + 1)
    {
        gaps++;
    }

    lastSequence = sequence;
    consumed++;
    priceSum += bid + ask + instrumentId;
}

using var aeron = Kit.Connect();
using var subscription = aeron.AddSubscription(channel, stream);
Console.WriteLine("READY");
Console.Out.Flush();
Console.Error.WriteLine($"[consumer-{label}] subscribed, awaiting image");

while (!subscription.IsConnected)
{
    Thread.Yield();
}

Console.Error.WriteLine($"[consumer-{label}] image attached");

var assembler = new FragmentAssembler(HandlerHelper.ToFragmentHandler(OnTick));
var deadline = Kit.NowNs() + durationSeconds * 1_000_000_000L;
var lastMessageAt = Kit.NowNs();
while (true)
{
    var now = Kit.NowNs();
    if (now >= deadline || (consumed > 0 && now - lastMessageAt > IdleExitNs))
    {
        break;
    }

    if (subscription.Poll(assembler, 1024) > 0)
    {
        lastMessageAt = now;
    }
}

long total = 0;
double weighted = 0;
for (var index = 0; index < Buckets; index++)
{
    total += counts[index];
    weighted += counts[index] * (index * (double)BucketNs + BucketNs / 2.0);
}

double Percentile(double fraction)
{
    if (total == 0)
    {
        return 0;
    }

    var target = Math.Max((long)(total * fraction), 1);
    long seen = 0;
    for (var index = 0; index < Buckets; index++)
    {
        seen += counts[index];
        if (seen >= target)
        {
            return (index * (double)BucketNs + BucketNs / 2.0) / 1000.0;
        }
    }

    return 0;
}

var mean = total > 0 ? weighted / total / 1000.0 : 0.0;
Console.Error.WriteLine($"[consumer-{label}] price_sum={priceSum}");
Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
    $"{{\"role\":\"consumer\",\"client\":\"{label}\",\"consumed\":{consumed},\"first_sequence\":{firstSequence}," +
    $"\"last_sequence\":{lastSequence},\"sequence_span\":{lastSequence - firstSequence + 1},\"gaps\":{gaps}," +
    $"\"p50_us\":{Percentile(0.5):F3},\"p90_us\":{Percentile(0.9):F3},\"p99_us\":{Percentile(0.99):F3}," +
    $"\"p999_us\":{Percentile(0.999):F3},\"max_us\":{maxNs / 1000.0:F3},\"mean_us\":{mean:F3},\"negative_latencies\":{negatives}}}"));
