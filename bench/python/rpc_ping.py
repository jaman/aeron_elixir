"""pyaeron requester for the RPC round-trip benchmark.

Encodes a 28-byte tick, sends it on the ping stream, waits for its echo on the
pong stream, decodes the echo and folds its fields into a running sum. After
the warmup it waits until the wall clock reaches RPC_START_AT_NS, then runs the
timed loop and emits one JSON line with the latency percentiles, round trips per
second and the wall-clock start and finish of the timed loop.
"""

import os
import struct
import sys
import time

import pyaeron

import payload_pool

TICK = struct.Struct("<Iqqq")
START_SPIN_NS = 2_000_000


def write_tick(buffer, sequence):
    instrument_id = sequence % 100
    bid = 100000 + (sequence % 1000)
    ask = bid + 10
    TICK.pack_into(buffer, 0, instrument_id, bid, ask, sequence)


def percentile(sorted_samples, pct):
    index = int(pct / 100.0 * len(sorted_samples))
    index = min(max(index, 0), len(sorted_samples) - 1)
    return sorted_samples[index]


def main():
    aeron_dir = os.environ.get("AERON_DIR", "/tmp/ae_drv")
    base = int(os.environ.get("RPC_STREAM_BASE", "7100"))
    pair_index = int(os.environ.get("RPC_PAIR_INDEX", "0"))
    length = int(os.environ.get("RPC_MESSAGE_LENGTH", "32"))
    warmup = int(os.environ.get("RPC_WARMUP_MESSAGES", "100000"))
    count = int(os.environ.get("RPC_MESSAGES", "1000000"))
    start_at_ns = int(os.environ.get("RPC_START_AT_NS", "0"))
    ping_stream = base + pair_index * 2
    pong_stream = base + pair_index * 2 + 1

    with pyaeron.Aeron(dir=aeron_dir) as aeron:
        pub = aeron.add_publication("aeron:ipc", ping_stream)
        sub = aeron.add_subscription("aeron:ipc", pong_stream)
        pub.await_connected()
        sub.await_connected()

        pool = payload_pool.build(length)
        entries = payload_pool.ENTRIES
        state = {"received": 0, "tick_sum": 0}

        def handler(view, header):
            state["received"] += 1
            if len(view) >= 28:
                instrument_id, bid, ask, sequence = TICK.unpack_from(view, 0)
                state["tick_sum"] += bid + instrument_id + ask + sequence

        offer = pub.offer
        poll = sub.poll
        now = time.perf_counter_ns

        def round_trip(sequence):
            message = pool[sequence % entries]
            write_tick(message, sequence)
            while not offer(message):
                pass
            state["received"] = 0
            while state["received"] == 0:
                poll(handler, 1)

        sequence = 0
        for _ in range(warmup):
            round_trip(sequence)
            sequence += 1

        samples = [0] * count
        sleep_ns = start_at_ns - time.time_ns() - START_SPIN_NS
        if sleep_ns > 0:
            time.sleep(sleep_ns / 1e9)
        while time.time_ns() < start_at_ns:
            pass
        started_at_ns = time.time_ns()
        start = now()
        for i in range(count):
            t0 = now()
            round_trip(sequence)
            sequence += 1
            samples[i] = now() - t0
        elapsed = now() - start
        finished_at_ns = time.time_ns()

        samples.sort()
        mean_ns = sum(samples) / count
        print(f"[bench-python-rpc] tick_sum={state['tick_sum']}", file=sys.stderr)

        print(
            '{"client":"python","scenario":"rpc",'
            f'"pairs":1,"pair_index":{pair_index},'
            f'"message_length":{length},'
            f'"samples":{count},'
            f'"elapsed_ms":{elapsed / 1e6:.3f},'
            f'"mean_us":{mean_ns / 1000.0:.4f},'
            f'"p50_us":{percentile(samples, 50) / 1000.0:.4f},'
            f'"p90_us":{percentile(samples, 90) / 1000.0:.4f},'
            f'"p99_us":{percentile(samples, 99) / 1000.0:.4f},'
            f'"p999_us":{percentile(samples, 99.9) / 1000.0:.4f},'
            f'"max_us":{samples[-1] / 1000.0:.4f},'
            f'"round_trips_per_sec":{count * 1e9 / elapsed:.3f},'
            f'"started_at_ns":{started_at_ns},'
            f'"finished_at_ns":{finished_at_ns}}}',
            flush=True,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
