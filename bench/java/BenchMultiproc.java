package io.aeron.samples.bench;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;

import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;

public final class BenchMultiproc
{
    private BenchMultiproc()
    {
    }

    public static void main(final String[] args) throws Exception
    {
        int workers = 4;
        int size = 256;
        int timeSeconds = 5;
        int warmupSeconds = 2;
        String aeronDir = System.getenv("AERON_DIR");
        if (aeronDir == null || aeronDir.isEmpty())
        {
            aeronDir = "/tmp/ae_drv";
        }

        for (final String arg : args)
        {
            final int eq = arg.indexOf('=');
            if (eq < 0)
            {
                continue;
            }
            final String key = arg.substring(0, eq);
            final String value = arg.substring(eq + 1);
            switch (key)
            {
                case "--workers":
                    workers = Integer.parseInt(value);
                    break;
                case "--size":
                    size = Integer.parseInt(value);
                    break;
                case "--time":
                    timeSeconds = Integer.parseInt(value);
                    break;
                case "--warmup":
                    warmupSeconds = Integer.parseInt(value);
                    break;
                default:
                    break;
            }
        }

        final String termLengthEnv = System.getenv("BENCH_TERM_LENGTH");
        final String termLength =
            (termLengthEnv != null && !termLengthEnv.isEmpty()) ? termLengthEnv : "16777216";

        System.err.println("[bench-java-mp] workers=" + workers + " size=" + size +
            " time=" + timeSeconds + "s warmup=" + warmupSeconds + "s dir=" + aeronDir);

        final Aeron.Context ctx = new Aeron.Context()
            .aeronDirectoryName(aeronDir)
            .idleStrategy(new YieldingIdleStrategy());

        try (Aeron aeron = Aeron.connect(ctx))
        {
            final Worker[] pool = new Worker[workers];
            final Thread[] threads = new Thread[workers];

            for (int i = 0; i < workers; i++)
            {
                final String channel = "aeron:ipc?alias=java-mp-" + i + "-" + System.nanoTime() +
                    "|term-length=" + termLength;
                final int streamId = (int)((System.nanoTime() + i) & 0x7FFF_FFFF);
                pool[i] = new Worker(aeron, channel, streamId, size, warmupSeconds, timeSeconds);
                threads[i] = new Thread(pool[i], "bench-worker-" + i);
            }

            for (final Thread thread : threads)
            {
                thread.start();
            }
            for (final Thread thread : threads)
            {
                thread.join();
            }

            long totalSent = 0;
            long totalReceived = 0;
            long maxElapsed = 0;
            for (final Worker worker : pool)
            {
                if (worker.failure != null)
                {
                    System.err.println("[bench-java-mp] worker failed: " + worker.failure);
                    System.exit(1);
                }
                totalSent += worker.sent;
                totalReceived += worker.received;
                maxElapsed = Math.max(maxElapsed, worker.elapsedNs);
            }

            final double opsPerSec = maxElapsed == 0 ? 0 : totalSent * 1_000_000_000.0 / maxElapsed;
            final StringBuilder sb = new StringBuilder();
            sb.append("{");
            sb.append("\"client\":\"java\",");
            sb.append("\"scenario\":\"throughput\",");
            sb.append("\"payload_size\":").append(size).append(",");
            sb.append("\"workers\":").append(workers).append(",");
            sb.append("\"samples\":").append(totalSent).append(",");
            sb.append("\"received\":").append(totalReceived).append(",");
            sb.append("\"ops_per_sec\":").append(opsPerSec).append(",");
            sb.append("\"bytes_per_sec\":").append(opsPerSec * size).append(",");
            sb.append("\"elapsed_ms\":").append(maxElapsed / 1_000_000.0);
            sb.append("}");
            System.out.println(sb);
        }
    }

    private static final class Worker implements Runnable
    {
        private final Aeron aeron;
        private final String channel;
        private final int streamId;
        private final int size;
        private final int warmupSeconds;
        private final int timeSeconds;

        long sent;
        long received;
        long elapsedNs;
        Throwable failure;

        Worker(
            final Aeron aeron, final String channel, final int streamId,
            final int size, final int warmupSeconds, final int timeSeconds)
        {
            this.aeron = aeron;
            this.channel = channel;
            this.streamId = streamId;
            this.size = size;
            this.warmupSeconds = warmupSeconds;
            this.timeSeconds = timeSeconds;
        }

        public void run()
        {
            try (Publication pub = aeron.addPublication(channel, streamId);
                 Subscription sub = aeron.addSubscription(channel, streamId))
            {
                final PayloadPool pool = new PayloadPool(size);

                waitForConnection(pub, sub);

                final long[] counters = new long[2];
                final FragmentHandler handler =
                    (buffer, offset, length, header) ->
                    {
                        counters[0]++;
                        if (length >= 28)
                        {
                            final int instrumentId = buffer.getInt(offset, ByteOrder.LITTLE_ENDIAN);
                            final long bid = buffer.getLong(offset + 4, ByteOrder.LITTLE_ENDIAN);
                            final long ask = buffer.getLong(offset + 12, ByteOrder.LITTLE_ENDIAN);
                            final long sequence = buffer.getLong(offset + 20, ByteOrder.LITTLE_ENDIAN);
                            counters[1] += bid + instrumentId + ask + sequence;
                        }
                    };

                long sequence = 0;
                final long warmupDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(warmupSeconds);
                while (System.nanoTime() < warmupDeadline)
                {
                    final UnsafeBuffer message = pool.entry(sequence);
                    writeTick(message, size, sequence++);
                    offer(pub, message);
                    sub.poll(handler, 1024);
                }
                while (sub.poll(handler, 1024) > 0)
                {
                }

                counters[0] = 0;
                counters[1] = 0;
                sequence = 0;
                long localSent = 0;
                final long startNs = System.nanoTime();
                final long runDeadline = startNs + TimeUnit.SECONDS.toNanos(timeSeconds);

                while (System.nanoTime() < runDeadline)
                {
                    for (int i = 0; i < 1_000; i++)
                    {
                        final UnsafeBuffer message = pool.entry(sequence);
                        writeTick(message, size, sequence++);
                        offerWithDrain(pub, sub, message, handler);
                        localSent++;
                    }
                    sub.poll(handler, 1024);
                }
                elapsedNs = System.nanoTime() - startNs;

                while (counters[0] < localSent)
                {
                    if (sub.poll(handler, 1024) == 0)
                    {
                        break;
                    }
                }

                sent = localSent;
                received = counters[0];
            }
            catch (final Throwable t)
            {
                failure = t;
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
    }
}
