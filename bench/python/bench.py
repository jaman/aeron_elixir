"""Aeron IPC benchmark for the pyaeron client.

Mirrors bench/c/bench.c: one publication and one subscription on a unique
``aeron:ipc`` channel in a single process. ``latency`` measures the
offer-to-poll round trip for one message at a time; ``throughput`` offers
messages in batches of 1000, draining the subscription on back-pressure.

Usage::

    python bench.py --mode=latency --size=32 --warmup=3 --time=10

Emits one JSON line on stdout in the same shape as the C, C++ and Java
benches so ``bench/run.exs`` can ingest it.
"""

import os
import struct
import sys
import time

import pyaeron

import payload_pool


DEFAULT_AERON_DIR = "/tmp/ae_drv"
DEFAULT_TERM_LENGTH = "16777216"
THROUGHPUT_BATCH = 1000
TICK_STRUCT = struct.Struct("<Iqqq")


def parse_args(argv):
    config = {
        "mode": "latency",
        "size": 32,
        "time": 10,
        "warmup": 3,
        "aeron_dir": os.environ.get("AERON_DIR", DEFAULT_AERON_DIR),
    }
    for arg in argv:
        if "=" not in arg:
            continue
        key, value = arg.split("=", 1)
        key = key.lstrip("-").replace("-", "_")
        if key in ("size", "time", "warmup"):
            config[key] = int(value)
        elif key in ("mode", "aeron_dir"):
            config[key] = value
    return config


def write_tick(buffer, size, sequence):
    if size < 28:
        return
    instrument_id = sequence % 100
    bid = 100000 + (sequence % 1000)
    ask = bid + 10
    TICK_STRUCT.pack_into(buffer, 0, instrument_id, bid, ask, sequence)


class Counter:
    __slots__ = ("count", "bid_sum")

    def __init__(self):
        self.count = 0
        self.bid_sum = 0


def make_counting_handler(counter):
    def handler(buffer_view, header):
        counter.count += 1

    return handler


def make_throughput_handler(counter):
    def handler(buffer_view, header):
        counter.count += 1
        if len(buffer_view) >= 28:
            instrument_id, bid, ask, sequence = TICK_STRUCT.unpack_from(buffer_view, 0)
            counter.bid_sum += bid + instrument_id + ask + sequence

    return handler


def offer_until_accepted(publication, payload):
    while not publication.offer(payload):
        pass


def offer_with_drain(publication, subscription, payload, handler):
    while not publication.offer(payload):
        subscription.poll(handler, 1024)


def drain_one(subscription, counter, handler):
    counter.count = 0
    while counter.count == 0:
        subscription.poll(handler, 1)


def drain_all(subscription, handler, max_polls=1000):
    total = 0
    for _ in range(max_polls):
        polled = subscription.poll(handler, 1024)
        total += polled
        if polled == 0:
            break
    return total


def percentile(sorted_samples, pct):
    if not sorted_samples:
        return 0.0
    index = int((pct / 100.0) * len(sorted_samples))
    index = min(max(index, 0), len(sorted_samples) - 1)
    return float(sorted_samples[index])


def run_latency(publication, subscription, pool, config):
    counter = Counter()
    handler = make_counting_handler(counter)
    now = time.perf_counter_ns
    size = config["size"]
    entries = payload_pool.ENTRIES
    sequence = 0

    warmup_deadline = now() + config["warmup"] * 1_000_000_000
    while now() < warmup_deadline:
        message = pool[sequence % entries]
        write_tick(message, size, sequence)
        sequence += 1
        offer_until_accepted(publication, message)
        drain_one(subscription, counter, handler)

    samples = []
    start_ns = now()
    run_deadline = start_ns + config["time"] * 1_000_000_000
    while now() < run_deadline:
        t0 = now()
        message = pool[sequence % entries]
        write_tick(message, size, sequence)
        sequence += 1
        offer_until_accepted(publication, message)
        drain_one(subscription, counter, handler)
        samples.append(now() - t0)
    elapsed_ns = now() - start_ns

    samples.sort()
    sample_count = len(samples)
    mean_ns = sum(samples) / sample_count if sample_count else 0.0

    print(
        '{"client":"python","scenario":"latency",'
        f'"payload_size":{config["size"]},'
        f'"samples":{sample_count},'
        f'"ops_per_sec":{sample_count * 1e9 / elapsed_ns if elapsed_ns else 0:.3f},'
        f'"elapsed_ms":{elapsed_ns / 1e6:.3f},'
        f'"mean_us":{mean_ns / 1000.0:.4f},'
        f'"median_us":{percentile(samples, 50) / 1000.0:.4f},'
        f'"p99_us":{percentile(samples, 99) / 1000.0:.4f},'
        f'"p999_us":{percentile(samples, 99.9) / 1000.0:.4f},'
        f'"min_us":{samples[0] / 1000.0 if samples else 0:.4f},'
        f'"max_us":{samples[-1] / 1000.0 if samples else 0:.4f}}}',
        flush=True,
    )


def run_throughput(publication, subscription, pool, config):
    counter = Counter()
    handler = make_throughput_handler(counter)
    size = config["size"]
    entries = payload_pool.ENTRIES
    now = time.perf_counter_ns
    sequence = 0

    warmup_deadline = now() + config["warmup"] * 1_000_000_000
    while now() < warmup_deadline:
        message = pool[sequence % entries]
        write_tick(message, size, sequence)
        sequence += 1
        offer_until_accepted(publication, message)
        subscription.poll(handler, 1024)

    counter.count = 0
    counter.bid_sum = 0
    sequence = 0
    sent = 0
    start_ns = now()
    run_deadline = start_ns + config["time"] * 1_000_000_000
    while now() < run_deadline:
        for _ in range(THROUGHPUT_BATCH):
            message = pool[sequence % entries]
            write_tick(message, size, sequence)
            sequence += 1
            offer_with_drain(publication, subscription, message, handler)
        sent += THROUGHPUT_BATCH
        subscription.poll(handler, 1024)
    elapsed_ns = now() - start_ns

    received = counter.count
    while received < sent:
        counter.count = 0
        polled = subscription.poll(handler, 1024)
        received += counter.count
        if polled == 0:
            break

    print(f"[bench-python] throughput bid_sum={counter.bid_sum}", file=sys.stderr)

    ops_per_sec = sent * 1e9 / elapsed_ns if elapsed_ns else 0.0
    print(
        '{"client":"python","scenario":"throughput",'
        f'"payload_size":{size},'
        f'"samples":{sent},'
        f'"ops_per_sec":{ops_per_sec:.3f},'
        f'"bytes_per_sec":{ops_per_sec * size:.3f},'
        f'"received":{received},'
        f'"elapsed_ms":{elapsed_ns / 1e6:.3f}}}',
        flush=True,
    )


def main(argv):
    config = parse_args(argv)
    term_length = os.environ.get("BENCH_TERM_LENGTH") or DEFAULT_TERM_LENGTH
    print(
        f"[bench-python] mode={config['mode']} size={config['size']} time={config['time']}s "
        f"warmup={config['warmup']}s dir={config['aeron_dir']} pyaeron={pyaeron.__version__} "
        f"aeron={pyaeron.aeron_version}",
        file=sys.stderr,
    )

    channel = f"aeron:ipc?alias=python-bench-{time.perf_counter_ns()}|term-length={term_length}"
    stream_id = time.perf_counter_ns() & 0x7FFFFFFF

    with pyaeron.Aeron(dir=config["aeron_dir"]) as aeron:
        publication = aeron.add_publication(channel, stream_id)
        subscription = aeron.add_subscription(channel, stream_id)
        publication.await_connected()
        subscription.await_connected()

        pool = payload_pool.build(config["size"])

        if config["mode"] == "latency":
            run_latency(publication, subscription, pool, config)
        elif config["mode"] == "throughput":
            run_throughput(publication, subscription, pool, config)
        else:
            print(f"unknown mode: {config['mode']}", file=sys.stderr)
            return 1

        subscription.close()
        publication.close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
