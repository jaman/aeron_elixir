package io.aeron.samples.bench;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.FragmentAssembler;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.BusySpinIdleStrategy;

import java.nio.ByteOrder;
import java.time.Instant;

public final class Consumer
{
    private static final int BUCKETS = 100_000;
    private static final int BUCKET_NS = 100;
    private static final long IDLE_EXIT_NS = 750_000_000L;

    private final long[] counts = new long[BUCKETS];
    private long consumed;
    private long firstSequence = -1;
    private long lastSequence = -1;
    private long gaps;
    private long priceSum;
    private long negatives;
    private long maxNs;

    private Consumer()
    {
    }

    private static final long CLOCK_GRID_NS = resolution();

    private static long resolution()
    {
        long grid = Long.MAX_VALUE;
        long previous = 0;
        for (int i = 0; i < 200_000; i++)
        {
            final Instant instant = Instant.now();
            final long value = instant.getEpochSecond() * 1_000_000_000L + instant.getNano();
            if (previous != 0 && value != previous)
            {
                final long delta = value - previous;
                if (delta > 0 && delta < grid)
                {
                    grid = delta;
                }
            }
            previous = value;
        }
        return grid == Long.MAX_VALUE ? 1 : grid;
    }

    private static long nowNs()
    {
        final Instant instant = Instant.now();
        return instant.getEpochSecond() * 1_000_000_000L + instant.getNano() + CLOCK_GRID_NS / 2;
    }

    private void onFragment(final org.agrona.DirectBuffer buffer, final int offset, final int length)
    {
        if (length < 36)
        {
            return;
        }

        final int instrumentId = buffer.getInt(offset, ByteOrder.LITTLE_ENDIAN);
        final long bid = buffer.getLong(offset + 4, ByteOrder.LITTLE_ENDIAN);
        final long ask = buffer.getLong(offset + 12, ByteOrder.LITTLE_ENDIAN);
        final long sequence = buffer.getLong(offset + 20, ByteOrder.LITTLE_ENDIAN);
        final long publishedAt = buffer.getLong(offset + 28, ByteOrder.LITTLE_ENDIAN);

        final long latency = nowNs() - publishedAt;
        if (latency < 0)
        {
            negatives++;
        }
        else
        {
            final long bucket = latency / BUCKET_NS;
            counts[(int)Math.min(bucket, BUCKETS - 1)]++;
            if (latency > maxNs)
            {
                maxNs = latency;
            }
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

    private double percentile(final long total, final double fraction)
    {
        if (total == 0)
        {
            return 0.0;
        }
        final long target = Math.max(1L, (long)(total * fraction));
        long seen = 0;
        for (int index = 0; index < BUCKETS; index++)
        {
            seen += counts[index];
            if (seen >= target)
            {
                return ((double)index * BUCKET_NS + BUCKET_NS / 2.0) / 1000.0;
            }
        }
        return 0.0;
    }

    private void run(final String aeronDir, final String channel, final int stream,
        final int durationSeconds, final String label)
    {
        final Aeron.Context ctx = new Aeron.Context()
            .aeronDirectoryName(aeronDir)
            .idleStrategy(new BusySpinIdleStrategy());

        try (Aeron aeron = Aeron.connect(ctx);
            Subscription subscription = aeron.addSubscription(channel, stream))
        {
            System.out.println("READY");
            System.out.flush();
            System.err.println("[consumer-" + label + "] subscribed, awaiting image");

            while (!subscription.isConnected())
            {
                Thread.onSpinWait();
            }
            System.err.println("[consumer-" + label + "] image attached");

            final FragmentHandler handler = new FragmentAssembler(
                (buffer, offset, length, header) -> onFragment(buffer, offset, length));

            final long deadline = nowNs() + durationSeconds * 1_000_000_000L;
            long lastMessageAt = nowNs();

            while (true)
            {
                final long current = nowNs();
                if (current >= deadline)
                {
                    break;
                }
                if (consumed > 0 && current - lastMessageAt > IDLE_EXIT_NS)
                {
                    break;
                }
                if (subscription.poll(handler, 1024) > 0)
                {
                    lastMessageAt = current;
                }
            }
        }

        long total = 0;
        double weighted = 0.0;
        for (int index = 0; index < BUCKETS; index++)
        {
            total += counts[index];
            weighted += (double)counts[index] * ((double)index * BUCKET_NS + BUCKET_NS / 2.0);
        }
        final double meanUs = total > 0 ? weighted / (double)total / 1000.0 : 0.0;

        System.err.println("[consumer-" + label + "] price_sum=" + priceSum);
        System.out.printf(
            "{\"role\":\"consumer\",\"client\":\"%s\",\"consumed\":%d," +
            "\"first_sequence\":%d,\"last_sequence\":%d,\"sequence_span\":%d,\"gaps\":%d," +
            "\"p50_us\":%.3f,\"p90_us\":%.3f,\"p99_us\":%.3f,\"p999_us\":%.3f," +
            "\"max_us\":%.3f,\"mean_us\":%.3f,\"negative_latencies\":%d}%n",
            label, consumed, firstSequence, lastSequence,
            lastSequence - firstSequence + 1, gaps,
            percentile(total, 0.5), percentile(total, 0.9),
            percentile(total, 0.99), percentile(total, 0.999),
            maxNs / 1000.0, meanUs, negatives);
        System.out.flush();
    }

    public static void main(final String[] args)
    {
        String aeronDir = System.getenv("AERON_DIR");
        if (aeronDir == null || aeronDir.isEmpty())
        {
            aeronDir = "/tmp/ae_drv";
        }

        String channel = System.getenv("HARNESS_CHANNEL");
        if (channel == null || channel.isEmpty())
        {
            channel = "aeron:ipc?alias=prices|term-length=67108864";
        }

        String streamText = System.getenv("HARNESS_STREAM");
        final int stream = (streamText == null || streamText.isEmpty()) ? 9001 : Integer.parseInt(streamText);

        String durationText = System.getenv("HARNESS_DURATION_S");
        final int duration = ((durationText == null || durationText.isEmpty())
            ? 30 : Integer.parseInt(durationText)) + 10;

        String label = System.getenv("HARNESS_LABEL");
        if (label == null || label.isEmpty())
        {
            label = "java";
        }

        new Consumer().run(aeronDir, channel, stream, duration, label);
    }
}
