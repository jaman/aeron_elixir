#ifndef AERON_BENCH_PAYLOAD_POOL_H
#define AERON_BENCH_PAYLOAD_POOL_H

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define PAYLOAD_POOL_ENTRIES 4096
#define PAYLOAD_POOL_SEED UINT64_C(0x5EEDAE20)

typedef struct payload_pool_s
{
    uint8_t *bytes;
    size_t size;
}
payload_pool_t;

static inline uint64_t payload_pool_next_word(uint64_t *state)
{
    *state += UINT64_C(0x9E3779B97F4A7C15);
    uint64_t mixed = *state;
    mixed = (mixed ^ (mixed >> 30)) * UINT64_C(0xBF58476D1CE4E5B9);
    mixed = (mixed ^ (mixed >> 27)) * UINT64_C(0x94D049BB133111EB);
    return mixed ^ (mixed >> 31);
}

static inline int payload_pool_init(payload_pool_t *pool, size_t size)
{
    size_t total = (size_t)PAYLOAD_POOL_ENTRIES * size;
    pool->size = size;
    pool->bytes = (uint8_t *)malloc(total);
    if (pool->bytes == NULL)
    {
        return -1;
    }

    uint64_t state = PAYLOAD_POOL_SEED;
    for (size_t offset = 0; offset < total; offset += 8)
    {
        uint64_t word = payload_pool_next_word(&state);
        uint8_t bytes[8];
        for (int index = 0; index < 8; index++)
        {
            bytes[index] = (uint8_t)(word >> (8 * index));
        }
        size_t remaining = total - offset;
        memcpy(pool->bytes + offset, bytes, remaining < 8 ? remaining : 8);
    }
    return 0;
}

static inline uint8_t *payload_pool_entry(const payload_pool_t *pool, int64_t sequence)
{
    return pool->bytes + (size_t)(sequence % PAYLOAD_POOL_ENTRIES) * pool->size;
}

static inline void payload_pool_close(payload_pool_t *pool)
{
    free(pool->bytes);
    pool->bytes = NULL;
}

#endif
