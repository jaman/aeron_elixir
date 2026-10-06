#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <inttypes.h>
#include <sched.h>
#include <signal.h>

#include "aeronc.h"
#include "payload_pool.h"

typedef struct rpc_config_s
{
    const char *mode;
    const char *aeron_dir;
    int32_t ping_stream;
    int32_t pong_stream;
    int length;
    int64_t warmup;
    int64_t messages;
    int64_t start_at_ns;
}
rpc_config_t;

typedef struct echo_state_s
{
    aeron_publication_t *publication;
    int64_t received;
    int64_t tick_sum;
}
echo_state_t;

#define START_SPIN_NS 2000000LL

static volatile sig_atomic_t running = 1;

static void handle_signal(int signal)
{
    (void)signal;
    running = 0;
}

static uint64_t now_ns(void)
{
#if defined(__APPLE__)
    return (uint64_t)clock_gettime_nsec_np(CLOCK_UPTIME_RAW);
#else
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
#endif
}

static int64_t realtime_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + (int64_t)ts.tv_nsec;
}

static void await_start(int64_t start_at_ns)
{
    int64_t sleep_ns = start_at_ns - realtime_ns() - START_SPIN_NS;
    if (sleep_ns > 0)
    {
        struct timespec duration = { (time_t)(sleep_ns / 1000000000LL), (long)(sleep_ns % 1000000000LL) };
        nanosleep(&duration, NULL);
    }
    while (realtime_ns() < start_at_ns)
    {
    }
}

static void write_tick(uint8_t *buf, int len, int64_t sequence)
{
    if (len < 28)
    {
        return;
    }
    uint32_t instrument_id = (uint32_t)(sequence % 100);
    int64_t bid = 100000 + (sequence % 1000);
    int64_t ask = bid + 10;
    memcpy(buf + 0, &instrument_id, sizeof(instrument_id));
    memcpy(buf + 4, &bid, sizeof(bid));
    memcpy(buf + 12, &ask, sizeof(ask));
    memcpy(buf + 20, &sequence, sizeof(sequence));
}

static int64_t read_tick(const uint8_t *buf, size_t length)
{
    if (length < 28)
    {
        return 0;
    }
    uint32_t instrument_id;
    int64_t bid;
    int64_t ask;
    int64_t sequence;
    memcpy(&instrument_id, buf + 0, sizeof(instrument_id));
    memcpy(&bid, buf + 4, sizeof(bid));
    memcpy(&ask, buf + 12, sizeof(ask));
    memcpy(&sequence, buf + 20, sizeof(sequence));
    return bid + (int64_t)instrument_id + ask + sequence;
}

static int env_int(const char *name, int fallback)
{
    const char *value = getenv(name);
    return (value != NULL && *value != '\0') ? atoi(value) : fallback;
}

static int64_t env_int64(const char *name, int64_t fallback)
{
    const char *value = getenv(name);
    return (value != NULL && *value != '\0') ? strtoll(value, NULL, 10) : fallback;
}

static void parse_args(int argc, char **argv, rpc_config_t *cfg)
{
    const char *dir = getenv("AERON_DIR");
    int base = env_int("RPC_STREAM_BASE", 7100);
    int pair_index = env_int("RPC_PAIR_INDEX", 0);

    cfg->mode = "ping";
    cfg->aeron_dir = (dir != NULL && *dir != '\0') ? dir : "/tmp/ae_drv";
    cfg->ping_stream = (int32_t)(base + pair_index * 2);
    cfg->pong_stream = (int32_t)(base + pair_index * 2 + 1);
    cfg->length = env_int("RPC_MESSAGE_LENGTH", 32);
    cfg->warmup = env_int64("RPC_WARMUP_MESSAGES", 100000);
    cfg->messages = env_int64("RPC_MESSAGES", 1000000);
    cfg->start_at_ns = env_int64("RPC_START_AT_NS", 0);

    for (int i = 1; i < argc; i++)
    {
        if (strncmp(argv[i], "--mode=", 7) == 0)
        {
            cfg->mode = argv[i] + 7;
        }
    }
}

static int compare_int64(const void *a, const void *b)
{
    int64_t va = *(const int64_t *)a;
    int64_t vb = *(const int64_t *)b;
    if (va < vb) return -1;
    if (va > vb) return 1;
    return 0;
}

static double percentile_us(const int64_t *sorted, int64_t count, double pct)
{
    int64_t index = (int64_t)(pct / 100.0 * (double)count);
    if (index >= count) index = count - 1;
    if (index < 0) index = 0;
    return (double)sorted[index] / 1000.0;
}

static void ping_handler(void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    (void)header;
    echo_state_t *state = (echo_state_t *)clientd;
    state->received++;
    state->tick_sum += read_tick(buffer, length);
}

static void pong_handler(void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    (void)header;
    echo_state_t *state = (echo_state_t *)clientd;
    state->received++;
    state->tick_sum += read_tick(buffer, length);

    while (running)
    {
        int64_t result = aeron_publication_offer(state->publication, buffer, length, NULL, NULL);
        if (result > 0)
        {
            return;
        }
        if (result == AERON_PUBLICATION_CLOSED || result == AERON_PUBLICATION_MAX_POSITION_EXCEEDED)
        {
            fprintf(stderr, "[bench-c-rpc] echo failed: %" PRId64 "\n", result);
            running = 0;
            return;
        }
    }
}

static int connect_client(const rpc_config_t *cfg, aeron_context_t **context, aeron_t **aeron)
{
    if (aeron_context_init(context) < 0 ||
        aeron_context_set_dir(*context, cfg->aeron_dir) < 0 ||
        aeron_init(aeron, *context) < 0 ||
        aeron_start(*aeron) < 0)
    {
        fprintf(stderr, "[bench-c-rpc] client init: %s\n", aeron_errmsg());
        return -1;
    }
    return 0;
}

static aeron_publication_t *add_publication(aeron_t *aeron, int32_t stream_id)
{
    aeron_async_add_publication_t *async = NULL;
    aeron_publication_t *publication = NULL;

    if (aeron_async_add_publication(&async, aeron, "aeron:ipc", stream_id) < 0)
    {
        fprintf(stderr, "[bench-c-rpc] add publication: %s\n", aeron_errmsg());
        return NULL;
    }
    while (publication == NULL)
    {
        if (aeron_async_add_publication_poll(&publication, async) < 0)
        {
            fprintf(stderr, "[bench-c-rpc] publication poll: %s\n", aeron_errmsg());
            return NULL;
        }
        sched_yield();
    }
    return publication;
}

static aeron_subscription_t *add_subscription(aeron_t *aeron, int32_t stream_id)
{
    aeron_async_add_subscription_t *async = NULL;
    aeron_subscription_t *subscription = NULL;

    if (aeron_async_add_subscription(&async, aeron, "aeron:ipc", stream_id, NULL, NULL, NULL, NULL) < 0)
    {
        fprintf(stderr, "[bench-c-rpc] add subscription: %s\n", aeron_errmsg());
        return NULL;
    }
    while (subscription == NULL)
    {
        if (aeron_async_add_subscription_poll(&subscription, async) < 0)
        {
            fprintf(stderr, "[bench-c-rpc] subscription poll: %s\n", aeron_errmsg());
            return NULL;
        }
        sched_yield();
    }
    return subscription;
}

static int await_connected(aeron_publication_t *pub, aeron_subscription_t *sub)
{
    uint64_t deadline = now_ns() + 30ULL * 1000000000ULL;
    while (!(aeron_publication_is_connected(pub) && aeron_subscription_is_connected(sub)))
    {
        if (now_ns() > deadline)
        {
            fprintf(stderr, "[bench-c-rpc] connection timeout\n");
            return -1;
        }
        sched_yield();
    }
    return 0;
}

static int run_ping(const rpc_config_t *cfg, aeron_t *aeron)
{
    aeron_publication_t *publication = add_publication(aeron, cfg->ping_stream);
    aeron_subscription_t *subscription = add_subscription(aeron, cfg->pong_stream);
    if (publication == NULL || subscription == NULL || await_connected(publication, subscription) < 0)
    {
        return 1;
    }

    payload_pool_t pool;
    if (payload_pool_init(&pool, (size_t)cfg->length) < 0)
    {
        fprintf(stderr, "[bench-c-rpc] payload pool allocation failed\n");
        return 1;
    }

    int64_t *samples = (int64_t *)malloc(sizeof(int64_t) * (size_t)cfg->messages);
    if (samples == NULL)
    {
        fprintf(stderr, "[bench-c-rpc] sample allocation failed\n");
        return 1;
    }

    echo_state_t state = { NULL, 0, 0 };
    int64_t sequence = 0;

    for (int64_t i = 0; i < cfg->warmup; i++)
    {
        uint8_t *message = payload_pool_entry(&pool, sequence);
        write_tick(message, cfg->length, sequence++);
        while (aeron_publication_offer(publication, message, (size_t)cfg->length, NULL, NULL) < 0)
        {
        }
        state.received = 0;
        while (state.received == 0)
        {
            aeron_subscription_poll(subscription, ping_handler, &state, 1);
        }
    }

    await_start(cfg->start_at_ns);
    int64_t started_at_ns = realtime_ns();
    uint64_t start_ns = now_ns();
    for (int64_t i = 0; i < cfg->messages; i++)
    {
        uint64_t t0 = now_ns();
        uint8_t *message = payload_pool_entry(&pool, sequence);
        write_tick(message, cfg->length, sequence++);
        while (aeron_publication_offer(publication, message, (size_t)cfg->length, NULL, NULL) < 0)
        {
        }
        state.received = 0;
        while (state.received == 0)
        {
            aeron_subscription_poll(subscription, ping_handler, &state, 1);
        }
        samples[i] = (int64_t)(now_ns() - t0);
    }
    uint64_t elapsed_ns = now_ns() - start_ns;
    int64_t finished_at_ns = realtime_ns();

    double mean_ns = 0;
    for (int64_t i = 0; i < cfg->messages; i++)
    {
        mean_ns += (double)samples[i];
    }
    mean_ns /= (double)cfg->messages;

    qsort(samples, (size_t)cfg->messages, sizeof(int64_t), compare_int64);

    fprintf(stderr, "[bench-c-rpc] tick_sum=%" PRId64 "\n", state.tick_sum);

    printf(
        "{\"client\":\"c\",\"scenario\":\"rpc\","
        "\"pairs\":1,"
        "\"message_length\":%d,"
        "\"samples\":%" PRId64 ","
        "\"elapsed_ms\":%.3f,"
        "\"mean_us\":%.4f,"
        "\"p50_us\":%.4f,"
        "\"p90_us\":%.4f,"
        "\"p99_us\":%.4f,"
        "\"p999_us\":%.4f,"
        "\"max_us\":%.4f,"
        "\"round_trips_per_sec\":%.3f,"
        "\"started_at_ns\":%" PRId64 ","
        "\"finished_at_ns\":%" PRId64 "}\n",
        cfg->length,
        cfg->messages,
        (double)elapsed_ns / 1e6,
        mean_ns / 1000.0,
        percentile_us(samples, cfg->messages, 50.0),
        percentile_us(samples, cfg->messages, 90.0),
        percentile_us(samples, cfg->messages, 99.0),
        percentile_us(samples, cfg->messages, 99.9),
        (double)samples[cfg->messages - 1] / 1000.0,
        (double)cfg->messages * 1e9 / (double)elapsed_ns,
        started_at_ns,
        finished_at_ns);
    fflush(stdout);

    free(samples);
    payload_pool_close(&pool);
    return 0;
}

static int run_pong(const rpc_config_t *cfg, aeron_t *aeron)
{
    aeron_subscription_t *subscription = add_subscription(aeron, cfg->ping_stream);
    aeron_publication_t *publication = add_publication(aeron, cfg->pong_stream);
    if (publication == NULL || subscription == NULL)
    {
        return 1;
    }

    printf("READY\n");
    fflush(stdout);

    if (await_connected(publication, subscription) < 0)
    {
        return 1;
    }

    echo_state_t state = { publication, 0, 0 };
    while (running)
    {
        aeron_subscription_poll(subscription, pong_handler, &state, 1);
    }

    fprintf(stderr, "[bench-c-rpc] pong tick_sum=%" PRId64 " received=%" PRId64 "\n",
        state.tick_sum, state.received);
    return 0;
}

int main(int argc, char **argv)
{
    rpc_config_t cfg;
    parse_args(argc, argv, &cfg);

    signal(SIGTERM, handle_signal);
    signal(SIGINT, handle_signal);

    fprintf(stderr, "[bench-c-rpc] mode=%s ping=%d pong=%d length=%d dir=%s\n",
        cfg.mode, cfg.ping_stream, cfg.pong_stream, cfg.length, cfg.aeron_dir);

    aeron_context_t *context = NULL;
    aeron_t *aeron = NULL;
    if (connect_client(&cfg, &context, &aeron) < 0)
    {
        return 1;
    }

    int result = strcmp(cfg.mode, "pong") == 0 ? run_pong(&cfg, aeron) : run_ping(&cfg, aeron);

    aeron_close(aeron);
    aeron_context_close(context);
    return result;
}
