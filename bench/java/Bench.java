package io.aeron.samples.bench;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;

import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class Bench
{
    private Bench()
    {
    }

    public static void main(final String[] args) throws Exception
    {
        final Config cfg = Config.parse(args);
        System.err.println("[bench-java] mode=" + cfg.mode + " size=" + cfg.size +
            " time=" + cfg.timeSeconds + "s warmup=" + cfg.warmupSeconds +
            "s dir=" + cfg.aeronDir);

        final Aeron.Context ctx = new Aeron.Context()
            .aeronDirectoryName(cfg.aeronDir)
            .idleStrategy(new YieldingIdleStrategy());

        try (Aeron aeron = Aeron.connect(ctx))
        {
            final String termLengthEnv = System.getenv("BENCH_TERM_LENGTH");
            final String termLength =
                (termLengthEnv != null && !termLengthEnv.isEmpty()) ? termLengthEnv : "16777216";
            final String channel =
                "aeron:ipc?alias=java-bench-" + System.nanoTime() + "|term-length=" + termLength;
            final int streamId = (int)(System.nanoTime() & 0x7FFF_FFFF);

            try (Publication pub = aeron.addPublication(channel, streamId);
                 Subscription sub = aeron.addSubscription(channel, streamId))
            {
                final PayloadPool pool = new PayloadPool(cfg.size);

                waitForConnection(pub, sub);

                switch (cfg.mode)
                {
                    case "latency":
                        runLatency(pub, sub, pool, cfg);
                        break;
                    case "throughput":
                        runThroughput(pub, sub, pool, cfg);
                        break;
                    default:
                        throw new IllegalArgumentException("mode=" + cfg.mode);
                }
            }
        }
    }

    private static void waitForConnection(final Publication pub, final Subscription sub)
        throws InterruptedException
    {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!pub.isConnected() || !sub.isConnected())
        {
            if (System.nanoTime() > deadline)
            {
                throw new IllegalStateException("publisher/subscriber never connected");
            }
            Thread.sleep(1);
        }
    }

    private static void runLatency(
        final Publication pub, final Subscription sub,
        final PayloadPool pool, final Config cfg)
    {
        final AtomicLong counter = new AtomicLong();
        long sequence = 0;
        final FragmentHandler handler =
            (buffer, offset, length, header) -> counter.incrementAndGet();

        final long[] samples = new long[cfg.timeSeconds * 10_000_000];
        int sampleCount = 0;
        final long warmupDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(cfg.warmupSeconds);

        while (System.nanoTime() < warmupDeadline)
        {
            counter.set(0);
            final UnsafeBuffer message = pool.entry(sequence);
            writeTick(message, cfg.size, sequence++);
            offer(pub, message);
            drainOne(sub, handler, counter);
        }

        final long startNs = System.nanoTime();
        final long runDeadline = startNs + TimeUnit.SECONDS.toNanos(cfg.timeSeconds);

        while (sampleCount < samples.length)
        {
            final long now = System.nanoTime();
            if (now >= runDeadline)
            {
                break;
            }
            counter.set(0);
            final long t0 = System.nanoTime();
            final UnsafeBuffer message = pool.entry(sequence);
            writeTick(message, cfg.size, sequence++);
            offer(pub, message);
            drainOne(sub, handler, counter);
            samples[sampleCount++] = System.nanoTime() - t0;
        }

        final long elapsedNs = System.nanoTime() - startNs;
        report("java", "latency", cfg.size, samples, sampleCount, elapsedNs);
    }

    private static void runThroughput(
        final Publication pub, final Subscription sub,
        final PayloadPool pool, final Config cfg)
    {
        final AtomicLong received = new AtomicLong();
        final AtomicLong bidSum = new AtomicLong();
        final FragmentHandler handler =
            (buffer, offset, length, header) ->
            {
                received.incrementAndGet();
                if (length >= 28)
                {
                    final int instrumentId = buffer.getInt(offset, ByteOrder.LITTLE_ENDIAN);
                    final long bid = buffer.getLong(offset + 4, ByteOrder.LITTLE_ENDIAN);
                    final long ask = buffer.getLong(offset + 12, ByteOrder.LITTLE_ENDIAN);
                    final long sequence = buffer.getLong(offset + 20, ByteOrder.LITTLE_ENDIAN);
                    bidSum.addAndGet(bid + instrumentId + ask + sequence);
                }
            };

        long sequence = 0;
        final long warmupDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(cfg.warmupSeconds);
        while (System.nanoTime() < warmupDeadline)
        {
            final UnsafeBuffer message = pool.entry(sequence);
            writeTick(message, cfg.size, sequence++);
            offer(pub, message);
            sub.poll(handler, 64);
        }
        received.set(0);
        bidSum.set(0);
        sequence = 0;

        final long runDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(cfg.timeSeconds);
        long sent = 0;
        long startNs = System.nanoTime();

        while (System.nanoTime() < runDeadline)
        {
            for (int i = 0; i < 1_000; i++)
            {
                final UnsafeBuffer message = pool.entry(sequence);
                writeTick(message, cfg.size, sequence++);
                offerWithDrain(pub, sub, message, handler);
                sent++;
            }
            sub.poll(handler, 1024);
        }

        final long elapsedNs = System.nanoTime() - startNs;

        while (received.get() < sent)
        {
            final int drained = sub.poll(handler, 1024);
            if (drained == 0)
            {
                break;
            }
        }

        System.err.println("[bench-java] throughput bid_sum=" + bidSum.get());
        report("java", "throughput", cfg.size, new long[]{sent, elapsedNs, received.get()});
    }

    private static void writeTick(final UnsafeBuffer payload, final int size, final long sequence)
    {
        if (size < 28)
        {
            return;
        }
        final int instrumentId = (int)(sequence % 100);
        final long bid = 100000 + (sequence % 1000);
        final long ask = bid + 10;
        payload.putInt(0, instrumentId, ByteOrder.LITTLE_ENDIAN);
        payload.putLong(4, bid, ByteOrder.LITTLE_ENDIAN);
        payload.putLong(12, ask, ByteOrder.LITTLE_ENDIAN);
        payload.putLong(20, sequence, ByteOrder.LITTLE_ENDIAN);
    }

    private static void offer(final Publication pub, final UnsafeBuffer payload)
    {
        while (true)
        {
            final long position = pub.offer(payload, 0, payload.capacity());
            if (position > 0)
            {
                return;
            }
            if (position == Publication.CLOSED || position == Publication.MAX_POSITION_EXCEEDED)
            {
                throw new RuntimeException("publication closed: " + position);
            }
        }
    }

    private static void offerWithDrain(
        final Publication pub, final Subscription sub,
        final UnsafeBuffer payload, final FragmentHandler handler)
    {
        while (true)
        {
            final long position = pub.offer(payload, 0, payload.capacity());
            if (position > 0)
            {
                return;
            }
            if (position == Publication.CLOSED || position == Publication.MAX_POSITION_EXCEEDED)
            {
                throw new RuntimeException("publication closed: " + position);
            }
            sub.poll(handler, 1024);
        }
    }

    private static void drainOne(
        final Subscription sub, final FragmentHandler handler, final AtomicLong counter)
    {
        while (counter.get() == 0)
        {
            sub.poll(handler, 1);
        }
    }

    private static void report(
        final String client, final String scenario, final int size,
        final long[] samples, final int sampleCount, final long elapsedNs)
    {
        final long[] copy = Arrays.copyOf(samples, sampleCount);
        Arrays.sort(copy);

        final double meanNs = sampleCount == 0 ? 0 : Arrays.stream(copy).average().orElse(0);
        final double medianNs = percentile(copy, 50);
        final double p99Ns = percentile(copy, 99);
        final double p999Ns = percentile(copy, 99.9);
        final double minNs = copy.length == 0 ? 0 : copy[0];
        final double maxNs = copy.length == 0 ? 0 : copy[copy.length - 1];
        final double opsPerSec = elapsedNs == 0 ? 0 : sampleCount * 1_000_000_000.0 / elapsedNs;

        final StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"client\":\"").append(client).append("\",");
        sb.append("\"scenario\":\"").append(scenario).append("\",");
        sb.append("\"payload_size\":").append(size).append(",");
        sb.append("\"samples\":").append(sampleCount).append(",");
        sb.append("\"ops_per_sec\":").append(opsPerSec).append(",");
        sb.append("\"elapsed_ms\":").append(elapsedNs / 1_000_000.0).append(",");
        sb.append("\"mean_us\":").append(meanNs / 1_000.0).append(",");
        sb.append("\"median_us\":").append(medianNs / 1_000.0).append(",");
        sb.append("\"p99_us\":").append(p99Ns / 1_000.0).append(",");
        sb.append("\"p999_us\":").append(p999Ns / 1_000.0).append(",");
        sb.append("\"min_us\":").append(minNs / 1_000.0).append(",");
        sb.append("\"max_us\":").append(maxNs / 1_000.0);
        sb.append("}");

        System.out.println(sb);
    }

    private static void report(
        final String client, final String scenario, final int size,
        final long[] sentElapsedReceived)
    {
        final long sent = sentElapsedReceived[0];
        final long elapsedNs = sentElapsedReceived[1];
        final long received = sentElapsedReceived[2];

        final double opsPerSec = elapsedNs == 0 ? 0 : sent * 1_000_000_000.0 / elapsedNs;
        final double bytesPerSec = opsPerSec * size;

        final StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"client\":\"").append(client).append("\",");
        sb.append("\"scenario\":\"").append(scenario).append("\",");
        sb.append("\"payload_size\":").append(size).append(",");
        sb.append("\"samples\":").append(sent).append(",");
        sb.append("\"ops_per_sec\":").append(opsPerSec).append(",");
        sb.append("\"bytes_per_sec\":").append(bytesPerSec).append(",");
        sb.append("\"received\":").append(received).append(",");
        sb.append("\"elapsed_ms\":").append(elapsedNs / 1_000_000.0);
        sb.append("}");

        System.out.println(sb);
    }

    private static double percentile(final long[] sorted, final double pct)
    {
        if (sorted.length == 0)
        {
            return 0;
        }
        final int idx = (int)Math.min(sorted.length - 1,
            Math.ceil((pct / 100.0) * sorted.length) - 1);
        return sorted[idx];
    }

    private static final class Config
    {
        final String mode;
        final int size;
        final int timeSeconds;
        final int warmupSeconds;
        final String aeronDir;

        Config(final String mode, final int size, final int timeSeconds,
            final int warmupSeconds, final String aeronDir)
        {
            this.mode = mode;
            this.size = size;
            this.timeSeconds = timeSeconds;
            this.warmupSeconds = warmupSeconds;
            this.aeronDir = aeronDir;
        }

        static Config parse(final String[] args)
        {
            String mode = "latency";
            int size = 32;
            int timeSeconds = 10;
            int warmupSeconds = 3;
            String aeronDir = System.getenv().getOrDefault("AERON_DIR", "/tmp/ae_drv");

            for (final String a : args)
            {
                final int eq = a.indexOf('=');
                if (eq <= 0)
                {
                    continue;
                }
                final String key = a.substring(0, eq);
                final String val = a.substring(eq + 1);
                switch (key)
                {
                    case "--mode": mode = val; break;
                    case "--size": size = Integer.parseInt(val); break;
                    case "--time": timeSeconds = Integer.parseInt(val); break;
                    case "--warmup": warmupSeconds = Integer.parseInt(val); break;
                    case "--aeron-dir": aeronDir = val; break;
                    default: break;
                }
            }

            return new Config(mode, size, timeSeconds, warmupSeconds, aeronDir);
        }
    }
}
