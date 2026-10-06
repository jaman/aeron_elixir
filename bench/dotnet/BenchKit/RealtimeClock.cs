using System.Runtime.InteropServices;

namespace AeronBench;

/// <summary>Reads CLOCK_REALTIME in nanoseconds since the Unix epoch, the clock the harness publisher stamps ticks with.</summary>
public static class RealtimeClock
{
    private const int ClockRealtime = 0;

    [StructLayout(LayoutKind.Sequential)]
    private struct TimeSpec
    {
        public long Seconds;
        public long Nanoseconds;
    }

    [DllImport("libc", EntryPoint = "clock_gettime")]
    private static extern int ClockGetTime(int clockId, out TimeSpec time);

    public static long NowNs()
    {
        ClockGetTime(ClockRealtime, out var time);
        return time.Seconds * 1_000_000_000L + time.Nanoseconds;
    }
}
