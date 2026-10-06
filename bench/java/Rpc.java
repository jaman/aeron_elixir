package io.aeron.samples.bench;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;

import java.nio.ByteOrder;
import java.util.Arrays;

public final class Rpc
{
    private static final long START_SPIN_NS = 2_000_000L;

    private Rpc()
    {
    }

    public static void main(final String[] args) throws Exception
    {
        String mode = "ping";
        for (final String arg : args)
        {
            if (arg.startsWith("--mode="))
            {
                mode = arg.substring(7);
            }
        }

        String aeronDir = System.getenv("AERON_DIR");
        if (aeronDir == null || aeronDir.isEmpty())
        {
            aeronDir = "/tmp/ae_drv";
        }

        final int base = envInt("RPC_STREAM_BASE", 7100);
        final int pairIndex = envInt("RPC_PAIR_INDEX", 0);
        final int pingStream = base + pairIndex * 2;
        final int pongStream = base + pairIndex * 2 + 1;
        final int length = envInt("RPC_MESSAGE_LENGTH", 32);
        final long warmup = envLong("RPC_WARMUP_MESSAGES", 100_000L);
        final long messages = envLong("RPC_MESSAGES", 1_000_000L);
        final long startAtNs = envLong("RPC_START_AT_NS", 0L);

        System.err.println("[bench-java-rpc] mode=" + mode + " ping=" + pingStream +
            " pong=" + pongStream + " length=" + length + " dir=" + aeronDir);

        final Aeron.Context ctx = new Aeron.Context()
            .aeronDirectoryName(aeronDir)
            .idleStrategy(new BusySpinIdleStrategy());

        try (Aeron aeron = Aeron.connect(ctx))
        {
            if ("pong".equals(mode))
            {
                runPong(aeron, pingStream, pongStream);
            }
            else
            {
                runPing(aeron, pingStream, pongStream, length, warmup, messages, startAtNs);
            }
        }
    }

    private static void runPing(
        final Aeron aeron, final int pingStream, final int pongStream,
        final int length, final long warmup, final long messages, final long startAtNs)
    {
        try (Publication pub = aeron.addPublication("aeron:ipc", pingStream);
             Subscription sub = aeron.addSubscription("aeron:ipc", pongStream))
        {
            awaitConnected(pub, sub);

            final PayloadPool pool = new PayloadPool(length);

            final long[] counters = new long[2];
            final FragmentHandler handler =
                (buffer, offset, len, header) ->
                {
                    counters[0]++;
                    counters[1] += readTick(buffer, offset, len);
                };

            final long[] samples = new long[(int)messages];
            long sequence = 0;

            for (long i = 0; i < warmup; i++)
            {
                roundTrip(pub, sub, pool.entry(sequence), length, sequence++, counters, handler);
            }

            awaitStart(startAtNs);
            final long startedAtNs = realtimeNs();
            final long startNs = System.nanoTime();
            for (int i = 0; i < messages; i++)
            {
                final long t0 = System.nanoTime();
                roundTrip(pub, sub, pool.entry(sequence), length, sequence++, counters, handler);
                samples[i] = System.nanoTime() - t0;
            }
            final long elapsedNs = System.nanoTime() - startNs;
            final long finishedAtNs = realtimeNs();

            double meanNs = 0;
            for (final long sample : samples)
            {
                meanNs += sample;
            }
            meanNs /= messages;

            Arrays.sort(samples);

            System.err.println("[bench-java-rpc] tick_sum=" + counters[1]);

            final StringBuilder sb = new StringBuilder();
            sb.append("{\"client\":\"java\",\"scenario\":\"rpc\",");
            sb.append("\"pairs\":1,");
            sb.append("\"message_length\":").append(length).append(",");
            sb.append("\"samples\":").append(messages).append(",");
            sb.append("\"elapsed_ms\":").append(elapsedNs / 1e6).append(",");
            sb.append("\"mean_us\":").append(meanNs / 1000.0).append(",");
            sb.append("\"p50_us\":").append(percentileUs(samples, 50.0)).append(",");
            sb.append("\"p90_us\":").append(percentileUs(samples, 90.0)).append(",");
            sb.append("\"p99_us\":").append(percentileUs(samples, 99.0)).append(",");
            sb.append("\"p999_us\":").append(percentileUs(samples, 99.9)).append(",");
            sb.append("\"max_us\":").append(samples[samples.length - 1] / 1000.0).append(",");
            sb.append("\"round_trips_per_sec\":").append(messages * 1e9 / elapsedNs).append(",");
            sb.append("\"started_at_ns\":").append(startedAtNs).append(",");
            sb.append("\"finished_at_ns\":").append(finishedAtNs);
            sb.append("}");
            System.out.println(sb);
            System.out.flush();
        }
    }

    private static void roundTrip(
        final Publication pub, final Subscription sub, final UnsafeBuffer payload,
        final int length, final long sequence, final long[] counters, final FragmentHandler handler)
    {
        writeTick(payload, length, sequence);
        while (pub.offer(payload, 0, length) < 0L)
        {
        }
        counters[0] = 0;
        while (counters[0] == 0)
        {
            sub.poll(handler, 1);
        }
    }

    private static void runPong(final Aeron aeron, final int pingStream, final int pongStream)
    {
        try (Subscription sub = aeron.addSubscription("aeron:ipc", pingStream);
             Publication pub = aeron.addPublication("aeron:ipc", pongStream))
        {
            System.out.println("READY");
            System.out.flush();

            awaitConnected(pub, sub);

            final long[] counters = new long[2];
            final FragmentHandler handler =
                (buffer, offset, len, header) ->
                {
                    counters[0]++;
                    counters[1] += readTick(buffer, offset, len);
                    while (pub.offer(buffer, offset, len) < 0L)
                    {
                    }
                };

            while (!Thread.currentThread().isInterrupted())
            {
                sub.poll(handler, 1);
            }
        }
    }

    private static void awaitConnected(final Publication pub, final Subscription sub)
    {
        final long deadline = System.nanoTime() + 30_000_000_000L;
        while (!(pub.isConnected() && sub.isConnected()))
        {
            if (System.nanoTime() > deadline)
            {
                throw new IllegalStateException("connection timeout");
            }
            Thread.yield();
        }
    }

    private static void writeTick(final UnsafeBuffer buffer, final int length, final long sequence)
    {
        if (length < 28)
        {
            return;
        }
        final int instrumentId = (int)(sequence % 100);
        final long bid = 100000 + (sequence % 1000);
        final long ask = bid + 10;
        buffer.putInt(0, instrumentId, ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(4, bid, ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(12, ask, ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(20, sequence, ByteOrder.LITTLE_ENDIAN);
    }

    private static long readTick(final org.agrona.DirectBuffer buffer, final int offset, final int length)
    {
        if (length < 28)
        {
            return 0L;
        }
        final int instrumentId = buffer.getInt(offset, ByteOrder.LITTLE_ENDIAN);
        final long bid = buffer.getLong(offset + 4, ByteOrder.LITTLE_ENDIAN);
        final long ask = buffer.getLong(offset + 12, ByteOrder.LITTLE_ENDIAN);
        final long sequence = buffer.getLong(offset + 20, ByteOrder.LITTLE_ENDIAN);
        return bid + instrumentId + ask + sequence;
    }

    private static double percentileUs(final long[] sorted, final double pct)
    {
        int index = (int)(pct / 100.0 * sorted.length);
        if (index >= sorted.length)
        {
            index = sorted.length - 1;
        }
        return sorted[index] / 1000.0;
    }

    private static long realtimeNs()
    {
        final java.time.Instant now = java.time.Instant.now();
        return now.getEpochSecond() * 1_000_000_000L + now.getNano();
    }

    private static void awaitStart(final long startAtNs)
    {
        final long sleepNs = startAtNs - realtimeNs() - START_SPIN_NS;
        if (sleepNs > 0)
        {
            try
            {
                Thread.sleep(sleepNs / 1_000_000L, (int)(sleepNs % 1_000_000L));
            }
            catch (final InterruptedException ex)
            {
                Thread.currentThread().interrupt();
            }
        }
        while (realtimeNs() < startAtNs)
        {
            Thread.onSpinWait();
        }
    }

    private static int envInt(final String name, final int fallback)
    {
        final String value = System.getenv(name);
        return (value != null && !value.isEmpty()) ? Integer.parseInt(value) : fallback;
    }

    private static long envLong(final String name, final long fallback)
    {
        final String value = System.getenv(name);
        return (value != null && !value.isEmpty()) ? Long.parseLong(value) : fallback;
    }
}
