"""Multi-worker Aeron IPC throughput benchmark for the pyaeron client.

Spawns ``--workers`` OS processes. Each process connects its own
``pyaeron.Aeron`` client to the media driver, adds one publication and one
subscription on a private ``aeron:ipc`` channel and runs the same
offer-with-drain loop as ``bench.py``'s throughput mode. Aggregate throughput is
the sum of sent messages over the slowest worker's elapsed time.

Usage::

    python bench_multiproc.py --workers=4 --size=256 --warmup=2 --time=5

Emits one JSON line on stdout.
"""

import multiprocessing
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
        "workers": 4,
        "size": 256,
        "time": 5,
        "warmup": 2,
        "aeron_dir": os.environ.get("AERON_DIR", DEFAULT_AERON_DIR),
    }
    for arg in argv:
        if "=" not in arg:
            continue
        key, value = arg.split("=", 1)
        key = key.lstrip("-").replace("-", "_")
        if key in ("workers", "size", "time", "warmup"):
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


def run_worker(worker_index, config, term_length, results):
    channel = (
        f"aeron:ipc?alias=python-mp-{worker_index}-{time.perf_counter_ns()}"
        f"|term-length={term_length}"
    )
    stream_id = (time.perf_counter_ns() + worker_index) & 0x7FFFFFFF
    size = config["size"]
    now = time.perf_counter_ns

    with pyaeron.Aeron(dir=config["aeron_dir"]) as aeron:
        publication = aeron.add_publication(channel, stream_id)
        subscription = aeron.add_subscription(channel, stream_id)
        publication.await_connected()
        subscription.await_connected()

        pool = payload_pool.build(size)
        entries = payload_pool.ENTRIES
        counter = Counter()
        handler = make_throughput_handler(counter)
        sequence = 0

        warmup_deadline = now() + config["warmup"] * 1_000_000_000
        while now() < warmup_deadline:
            message = pool[sequence % entries]
            write_tick(message, size, sequence)
            sequence += 1
            offer_until_accepted(publication, message)
            subscription.poll(handler, 1024)

        while subscription.poll(handler, 1024) > 0:
            pass

        counter.count = 0
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

        while counter.count < sent:
            if subscription.poll(handler, 1024) == 0:
                break

        results.put((worker_index, sent, counter.count, elapsed_ns))
        subscription.close()
        publication.close()


def main(argv):
    config = parse_args(argv)
    term_length = os.environ.get("BENCH_TERM_LENGTH") or DEFAULT_TERM_LENGTH
    workers = config["workers"]
    print(
        f"[bench-python-mp] workers={workers} size={config['size']} time={config['time']}s "
        f"warmup={config['warmup']}s dir={config['aeron_dir']}",
        file=sys.stderr,
    )

    context = multiprocessing.get_context("spawn")
    results = context.Queue()
    processes = [
        context.Process(target=run_worker, args=(index, config, term_length, results))
        for index in range(workers)
    ]
    for process in processes:
        process.start()

    collected = [results.get() for _ in range(workers)]
    for process in processes:
        process.join()

    total_sent = sum(sent for _, sent, _, _ in collected)
    total_received = sum(received for _, _, received, _ in collected)
    max_elapsed = max(elapsed for _, _, _, elapsed in collected)
    ops_per_sec = total_sent * 1e9 / max_elapsed if max_elapsed else 0.0

    print(
        '{"client":"python","scenario":"throughput",'
        f'"payload_size":{config["size"]},'
        f'"workers":{workers},'
        f'"samples":{total_sent},'
        f'"received":{total_received},'
        f'"ops_per_sec":{ops_per_sec:.3f},'
        f'"bytes_per_sec":{ops_per_sec * config["size"]:.3f},'
        f'"elapsed_ms":{max_elapsed / 1e6:.3f}}}',
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
