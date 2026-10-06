"""Payload pool shared by the Python benches.

``build(size)`` returns ENTRIES mutable payloads of ``size`` bytes, cut in order
from one byte stream of splitmix64 outputs (seed 0x5EEDAE20) written
little-endian; the C, C++, Java, Elixir and Go benches build the same bytes.
Message ``i`` is ``pool[i % ENTRIES]`` with the tick for ``i`` written over its
first 28 bytes.
"""

ENTRIES = 4096
SEED = 0x5EEDAE20
MASK = (1 << 64) - 1


def build(size):
    total = ENTRIES * size
    words = (total + 7) // 8
    stream = bytearray(words * 8)
    state = SEED
    for index in range(words):
        state = (state + 0x9E3779B97F4A7C15) & MASK
        mixed = ((state ^ (state >> 30)) * 0xBF58476D1CE4E5B9) & MASK
        mixed = ((mixed ^ (mixed >> 27)) * 0x94D049BB133111EB) & MASK
        mixed ^= mixed >> 31
        stream[index * 8:index * 8 + 8] = mixed.to_bytes(8, "little")
    return [bytearray(stream[index * size:(index + 1) * size]) for index in range(ENTRIES)]
