"""pyaeron responder for the RPC round-trip benchmark.

Subscribes to the ping stream, decodes each request's tick fields into a running
sum, then echoes the message onto the pong stream. Runs until killed. Prints
READY once both sides are connected.
"""

import os
import struct
import sys

import pyaeron

TICK = struct.Struct("<Iqqq")


def main():
    aeron_dir = os.environ.get("AERON_DIR", "/tmp/ae_drv")
    base = int(os.environ.get("RPC_STREAM_BASE", "7100"))
    pair_index = int(os.environ.get("RPC_PAIR_INDEX", "0"))
    ping_stream = base + pair_index * 2
    pong_stream = base + pair_index * 2 + 1

    with pyaeron.Aeron(dir=aeron_dir) as aeron:
        sub = aeron.add_subscription("aeron:ipc", ping_stream)
        pub = aeron.add_publication("aeron:ipc", pong_stream)
        print("READY", flush=True)
        sub.await_connected()
        pub.await_connected()

        state = {"tick_sum": 0}

        def handler(view, header):
            payload = view.tobytes()
            if len(payload) >= 28:
                instrument_id, bid, ask, sequence = TICK.unpack_from(payload, 0)
                state["tick_sum"] += bid + instrument_id + ask + sequence
            while not pub.offer(payload):
                pass

        while True:
            sub.poll(handler, 1)


if __name__ == "__main__":
    sys.exit(main())
