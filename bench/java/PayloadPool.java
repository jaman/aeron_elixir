package io.aeron.samples.bench;

import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;

final class PayloadPool
{
    static final int ENTRIES = 4096;
    private static final long SEED = 0x5EEDAE20L;

    private final UnsafeBuffer[] entries = new UnsafeBuffer[ENTRIES];

    PayloadPool(final int size)
    {
        final int total = ENTRIES * size;
        final UnsafeBuffer bytes = new UnsafeBuffer(BufferUtil.allocateDirectAligned(total, 64));

        long state = SEED;
        for (int offset = 0; offset < total; offset += 8)
        {
            state += 0x9E3779B97F4A7C15L;
            long mixed = state;
            mixed = (mixed ^ (mixed >>> 30)) * 0xBF58476D1CE4E5B9L;
            mixed = (mixed ^ (mixed >>> 27)) * 0x94D049BB133111EBL;
            mixed ^= mixed >>> 31;
            for (int index = 0; index < 8 && offset + index < total; index++)
            {
                bytes.putByte(offset + index, (byte)(mixed >>> (8 * index)));
            }
        }

        for (int index = 0; index < ENTRIES; index++)
        {
            entries[index] = new UnsafeBuffer(bytes, index * size, size);
        }
    }

    UnsafeBuffer entry(final long sequence)
    {
        return entries[(int)(sequence % ENTRIES)];
    }
}
