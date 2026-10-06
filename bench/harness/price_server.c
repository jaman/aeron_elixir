#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include <inttypes.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "aeronc.h"
#include "../c/payload_pool.h"

#define TICK_LENGTH 64
#define FIELDS_LENGTH 36
#define BATCH 200
#define CONNECT_TIMEOUT_NS 60000000000LL

typedef struct price_server_s
{
    aeron_publication_t *publication;
    payload_pool_t pool;
    int64_t *mids;
    int32_t instruments;
    uint64_t random_state;
    int64_t sequence;
    int64_t back_pressured_ns;
}
price_server_t;

static int64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + (int64_t)ts.tv_nsec;
}

static const char *env_or(const char *name, const char *fallback)
{
    const char *value = getenv(name);
    return (value == NULL || value[0] == '\0') ? fallback : value;
}

static uint64_t next_random(uint64_t *state)
{
    uint64_t value = *state;
    value ^= value << 13;
    value ^= value >> 7;
    value ^= value << 17;
    *state = value;
    return value;
}

static int64_t step_mid(price_server_t *server, int32_t instrument_id)
{
    int64_t moved = server->mids[instrument_id] + (int64_t)(next_random(&server->random_state) % 7) - 3;
    server->mids[instrument_id] = moved;
    return moved;
}

static void encode_tick(price_server_t *server, uint8_t *tick)
{
    int64_t sequence = server->sequence;
    uint32_t instrument_id = (uint32_t)(sequence % server->instruments);
    int64_t bid = step_mid(server, (int32_t)instrument_id);
    int64_t ask = bid + 2;
    int64_t published_at = now_ns();

    memcpy(tick + 0, &instrument_id, sizeof(instrument_id));
    memcpy(tick + 4, &bid, sizeof(bid));
    memcpy(tick + 12, &ask, sizeof(ask));
    memcpy(tick + 20, &sequence, sizeof(sequence));
    memcpy(tick + 28, &published_at, sizeof(published_at));
    memcpy(tick + FIELDS_LENGTH, payload_pool_entry(&server->pool, sequence) + FIELDS_LENGTH,
           TICK_LENGTH - FIELDS_LENGTH);
}

static int publish_one(price_server_t *server)
{
    uint8_t tick[TICK_LENGTH];
    encode_tick(server, tick);

    int64_t result = aeron_publication_offer(server->publication, tick, TICK_LENGTH, NULL, NULL);
    if (result > 0)
    {
        server->sequence++;
        return 0;
    }

    int64_t blocked_from = now_ns();
    while (result < 0)
    {
        if (result == AERON_PUBLICATION_CLOSED || result == AERON_PUBLICATION_MAX_POSITION_EXCEEDED)
        {
            fprintf(stderr, "[price-server] publication failed: %" PRId64 "\n", result);
            return -1;
        }
        sched_yield();
        result = aeron_publication_offer(server->publication, tick, TICK_LENGTH, NULL, NULL);
    }
    server->back_pressured_ns += now_ns() - blocked_from;
    server->sequence++;
    return 0;
}

static int publish_batch(price_server_t *server, int64_t count)
{
    for (int64_t index = 0; index < count; index++)
    {
        if (publish_one(server) < 0)
        {
            return -1;
        }
    }
    return 0;
}

static int run_flat_out(price_server_t *server, int64_t deadline)
{
    while (now_ns() < deadline)
    {
        if (publish_batch(server, BATCH) < 0)
        {
            return -1;
        }
    }
    return 0;
}

static int run_paced(price_server_t *server, int64_t rate, int64_t started_at, int64_t deadline)
{
    for (int64_t now = now_ns(); now < deadline; now = now_ns())
    {
        int64_t target = (int64_t)((double)(now - started_at) * (double)rate / 1e9);
        int64_t behind = target - server->sequence;
        if (behind > 0 && publish_batch(server, behind < BATCH ? behind : BATCH) < 0)
        {
            return -1;
        }
    }
    return 0;
}

static int await_publication(aeron_t *aeron, const char *channel, int32_t stream, aeron_publication_t **publication)
{
    aeron_async_add_publication_t *async = NULL;
    if (aeron_async_add_publication(&async, aeron, channel, stream) < 0)
    {
        return -1;
    }
    while (*publication == NULL)
    {
        if (aeron_async_add_publication_poll(publication, async) < 0)
        {
            return -1;
        }
        sched_yield();
    }
    return 0;
}

static int await_subscriber(aeron_publication_t *publication)
{
    int64_t deadline = now_ns() + CONNECT_TIMEOUT_NS;
    while (!aeron_publication_is_connected(publication))
    {
        if (now_ns() > deadline)
        {
            return -1;
        }
        sched_yield();
    }
    return 0;
}

int main(void)
{
    const char *aeron_dir = env_or("AERON_DIR", "/tmp/ae_drv");
    const char *channel = env_or("HARNESS_CHANNEL", "aeron:ipc?alias=prices|term-length=67108864");
    const int32_t stream = (int32_t)atoi(env_or("HARNESS_STREAM", "9001"));
    const int32_t instruments = (int32_t)atoi(env_or("HARNESS_INSTRUMENTS", "500"));
    const int64_t duration_s = (int64_t)atoi(env_or("HARNESS_DURATION_S", "30"));
    const char *rate_mode = env_or("HARNESS_RATE", "max");
    const int64_t rate = strcmp(rate_mode, "max") == 0 ? 0 : strtoll(rate_mode, NULL, 10);

    price_server_t server;
    memset(&server, 0, sizeof(server));
    server.instruments = instruments;
    server.random_state = UINT64_C(0x9E3779B97F4A7C15);
    server.mids = (int64_t *)calloc((size_t)instruments, sizeof(int64_t));
    if (server.mids == NULL || payload_pool_init(&server.pool, TICK_LENGTH) < 0)
    {
        fprintf(stderr, "[price-server] out of memory\n");
        return 1;
    }
    for (int32_t id = 0; id < instruments; id++)
    {
        server.mids[id] = 100000 + (id * 37) % 5000;
    }

    aeron_context_t *context = NULL;
    aeron_t *aeron = NULL;
    if (aeron_context_init(&context) < 0 ||
        aeron_context_set_dir(context, aeron_dir) < 0 ||
        aeron_init(&aeron, context) < 0 ||
        aeron_start(aeron) < 0 ||
        await_publication(aeron, channel, stream, &server.publication) < 0)
    {
        fprintf(stderr, "[price-server] init: %s\n", aeron_errmsg());
        return 1;
    }

    fprintf(stderr, "[price-server] awaiting subscriber\n");
    if (await_subscriber(server.publication) < 0)
    {
        fprintf(stderr, "[price-server] no subscriber connected\n");
        return 1;
    }
    fprintf(stderr, "[price-server] connected, publishing rate=%s\n", rate_mode);

    const int64_t started_at = now_ns();
    const int64_t deadline = started_at + duration_s * 1000000000LL;
    int status = rate == 0 ? run_flat_out(&server, deadline) : run_paced(&server, rate, started_at, deadline);
    const int64_t elapsed_ns = now_ns() - started_at;

    printf(
        "{\"role\":\"publisher\",\"rate_mode\":\"%s\",\"published\":%" PRId64 ","
        "\"achieved_rate\":%.1f,\"elapsed_ms\":%.1f,\"back_pressured_ms\":%.1f}\n",
        rate_mode,
        server.sequence,
        (double)server.sequence * 1e9 / (double)(elapsed_ns > 0 ? elapsed_ns : 1),
        (double)elapsed_ns / 1e6,
        (double)server.back_pressured_ns / 1e6);
    fflush(stdout);

    aeron_publication_close(server.publication, NULL, NULL);
    aeron_close(aeron);
    aeron_context_close(context);
    payload_pool_close(&server.pool);
    free(server.mids);
    return status == 0 ? 0 : 1;
}
