"""pyaeron consumer for the market-data harness.

Subscribes to the price stream, decodes every tick and records the latency
between the publisher's timestamp and receipt. Prints READY once subscribed and
one JSON line when the stream goes idle or the deadline passes.
"""

import os
import struct
import sys
import time

import pyaeron

TICK = struct.Struct("<IqqqQ")
BUCKETS = 100_000
BUCKET_NS = 100
IDLE_EXIT_NS = 750_000_000


def percentile(counts, total, fraction):
    if total == 0:
        return 0.0
    target = max(1, -(-int(total * fraction * 1000) // 1000))
    seen = 0
    for index, count in enumerate(counts):
        seen += count
        if seen >= target:
            return round((index * BUCKET_NS + BUCKET_NS // 2) / 1000.0, 3)
    return 0.0


def main():
    aeron_dir = os.environ.get("AERON_DIR", "/tmp/ae_drv")
    channel = os.environ.get("HARNESS_CHANNEL", "aeron:ipc?alias=prices|term-length=67108864")
    stream = int(os.environ.get("HARNESS_STREAM", "9001"))
    duration_s = int(os.environ.get("HARNESS_DURATION_S", "30")) + 10
    label = os.environ.get("HARNESS_LABEL", "python")

    counts = [0] * BUCKETS
    state = {
        "consumed": 0,
        "first": -1,
        "last": -1,
        "gaps": 0,
        "price_sum": 0,
        "negatives": 0,
        "max_ns": 0,
    }

    now = time.time_ns

    def handler(view, header):
        payload = view.tobytes()
        if len(payload) < 36:
            return
        instrument_id, bid, ask, sequence, published_at = TICK.unpack_from(payload, 0)
        latency = now() - published_at
        if latency < 0:
            state["negatives"] += 1
        else:
            counts[min(latency // BUCKET_NS, BUCKETS - 1)] += 1
            if latency > state["max_ns"]:
                state["max_ns"] = latency
        if state["first"] < 0:
            state["first"] = sequence
        elif sequence != state["last"] + 1:
            state["gaps"] += 1
        state["last"] = sequence
        state["consumed"] += 1
        state["price_sum"] += bid + ask + instrument_id

    with pyaeron.Aeron(dir=aeron_dir) as aeron:
        sub = aeron.add_subscription(channel, stream)
        print("READY", flush=True)
        print(f"[consumer-{label}] subscribed, awaiting image", file=sys.stderr)
        sub.await_connected()
        print(f"[consumer-{label}] image attached", file=sys.stderr)

        deadline = now() + duration_s * 1_000_000_000
        last_message_at = now()
        poll = sub.poll

        while True:
            current = now()
            if current >= deadline:
                break
            if state["consumed"] > 0 and current - last_message_at > IDLE_EXIT_NS:
                break
            if poll(handler, 1024) > 0:
                last_message_at = current

    total = sum(counts)
    mean_ns = 0.0
    if total:
        mean_ns = sum(c * (i * BUCKET_NS + BUCKET_NS // 2) for i, c in enumerate(counts)) / total

    print(f"[consumer-{label}] price_sum={state['price_sum']}", file=sys.stderr)
    print(
        '{"role":"consumer","client":"%s","consumed":%d,'
        '"first_sequence":%d,"last_sequence":%d,"sequence_span":%d,"gaps":%d,'
        '"p50_us":%s,"p90_us":%s,"p99_us":%s,"p999_us":%s,'
        '"max_us":%s,"mean_us":%s,"negative_latencies":%d}'
        % (
            label,
            state["consumed"],
            state["first"],
            state["last"],
            state["last"] - state["first"] + 1,
            state["gaps"],
            percentile(counts, total, 0.5),
            percentile(counts, total, 0.9),
            percentile(counts, total, 0.99),
            percentile(counts, total, 0.999),
            round(state["max_ns"] / 1000.0, 3),
            round(mean_ns / 1000.0, 3),
            state["negatives"],
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
