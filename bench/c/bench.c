#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>
#include <inttypes.h>
#include <sched.h>

#include "aeronc.h"
#include "concurrent/aeron_atomic.h"
#include "payload_pool.h"

typedef struct bench_config_s
{
    const char *mode;
    int size;
    int time_seconds;
    int warmup_seconds;
    const char *aeron_dir;
}
bench_config_t;

typedef struct counter_state_s
{
    int64_t count;
    volatile int64_t bid_sum;
}
counter_state_t;

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

static void parse_args(int argc, char **argv, bench_config_t *cfg)
{
    cfg->mode = "latency";
    cfg->size = 32;
    cfg->time_seconds = 10;
    cfg->warmup_seconds = 3;
    cfg->aeron_dir = getenv("AERON_DIR");
    if (cfg->aeron_dir == NULL)
    {
        cfg->aeron_dir = "/tmp/ae_drv";
    }

    for (int i = 1; i < argc; i++)
    {
        const char *eq = strchr(argv[i], '=');
        if (eq == NULL)
        {
            continue;
        }
        ptrdiff_t keylen = eq - argv[i];
        const char *val = eq + 1;
        if (keylen == 6 && strncmp(argv[i], "--mode", 6) == 0)
        {
            cfg->mode = val;
        }
        else if (keylen == 6 && strncmp(argv[i], "--size", 6) == 0)
        {
            cfg->size = atoi(val);
        }
        else if (keylen == 6 && strncmp(argv[i], "--time", 6) == 0)
        {
            cfg->time_seconds = atoi(val);
        }
        else if (keylen == 8 && strncmp(argv[i], "--warmup", 8) == 0)
        {
            cfg->warmup_seconds = atoi(val);
        }
        else if (keylen == 11 && strncmp(argv[i], "--aeron-dir", 11) == 0)
        {
            cfg->aeron_dir = val;
        }
    }
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

static void fragment_handler(void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    (void)buffer;
    (void)length;
    (void)header;
    counter_state_t *state = (counter_state_t *)clientd;
    state->count++;
}

static void throughput_fragment_handler(void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    (void)header;
    counter_state_t *state = (counter_state_t *)clientd;
    state->count++;
    if (length >= 28)
    {
        uint32_t instrument_id;
        int64_t bid;
        int64_t ask;
        int64_t sequence;
        memcpy(&instrument_id, buffer + 0, sizeof(instrument_id));
        memcpy(&bid, buffer + 4, sizeof(bid));
        memcpy(&ask, buffer + 12, sizeof(ask));
        memcpy(&sequence, buffer + 20, sizeof(sequence));
        state->bid_sum += bid + (int64_t)instrument_id + ask + sequence;
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

static double percentile(int64_t *sorted, int n, double pct)
{
    if (n == 0)
    {
        return 0.0;
    }
    int idx = (int)((pct / 100.0) * (double)n);
    if (idx >= n) idx = n - 1;
    if (idx < 0) idx = 0;
    return (double)sorted[idx];
}

static void offer_until_accepted(aeron_publication_t *pub, const uint8_t *buf, int len)
{
    while (1)
    {
        int64_t result = aeron_publication_offer(pub, buf, (size_t)len, NULL, NULL);
        if (result > 0)
        {
            return;
        }
        if (result == AERON_PUBLICATION_CLOSED || result == AERON_PUBLICATION_MAX_POSITION_EXCEEDED)
        {
            fprintf(stderr, "publication closed: %" PRId64 "\n", result);
            exit(1);
        }
    }
}

static void offer_with_drain(
    aeron_publication_t *pub,
    aeron_subscription_t *sub,
    const uint8_t *buf,
    int len,
    counter_state_t *state)
{
    while (1)
    {
        int64_t result = aeron_publication_offer(pub, buf, (size_t)len, NULL, NULL);
        if (result > 0)
        {
            return;
        }
        if (result == AERON_PUBLICATION_CLOSED || result == AERON_PUBLICATION_MAX_POSITION_EXCEEDED)
        {
            fprintf(stderr, "publication closed: %" PRId64 "\n", result);
            exit(1);
        }
        aeron_subscription_poll(sub, throughput_fragment_handler, state, 1024);
    }
}

static void wait_for_connection(aeron_publication_t *pub, aeron_subscription_t *sub)
{
    uint64_t deadline = now_ns() + 10ULL * 1000000000ULL;
    while (!(aeron_publication_is_connected(pub) && aeron_subscription_is_connected(sub)))
    {
        if (now_ns() > deadline)
        {
            fprintf(stderr, "connection timeout\n");
            exit(1);
        }
        sched_yield();
    }
}

static void drain_one(aeron_subscription_t *sub, counter_state_t *state)
{
    state->count = 0;
    while (state->count == 0)
    {
        aeron_subscription_poll(sub, fragment_handler, state, 1);
    }
}

static int drain_all(aeron_subscription_t *sub, int max_polls)
{
    counter_state_t state = { 0 };
    int total = 0;
    for (int i = 0; i < max_polls; i++)
    {
        state.count = 0;
        int n = aeron_subscription_poll(sub, fragment_handler, &state, 1024);
        total += n;
        if (n == 0)
        {
            break;
        }
    }
    return total;
}

static void run_latency(
    aeron_publication_t *pub,
    aeron_subscription_t *sub,
    const payload_pool_t *pool,
    const bench_config_t *cfg)
{
    counter_state_t state = { 0 };
    int64_t sequence = 0;

    int max_samples = cfg->time_seconds * 10000000;
    int64_t *samples = (int64_t *)malloc(sizeof(int64_t) * (size_t)max_samples);
    if (samples == NULL)
    {
        fprintf(stderr, "alloc failed\n");
        exit(1);
    }
    int sample_count = 0;

    uint64_t warmup_deadline = now_ns() + (uint64_t)cfg->warmup_seconds * 1000000000ULL;
    while (now_ns() < warmup_deadline)
    {
        state.count = 0;
        uint8_t *message = payload_pool_entry(pool, sequence);
        write_tick(message, cfg->size, sequence++);
        offer_until_accepted(pub, message, cfg->size);
        drain_one(sub, &state);
    }

    uint64_t start_ns = now_ns();
    uint64_t run_deadline = start_ns + (uint64_t)cfg->time_seconds * 1000000000ULL;

    while (sample_count < max_samples)
    {
        if (now_ns() >= run_deadline)
        {
            break;
        }
        state.count = 0;
        uint64_t t0 = now_ns();
        uint8_t *message = payload_pool_entry(pool, sequence);
        write_tick(message, cfg->size, sequence++);
        offer_until_accepted(pub, message, cfg->size);
        drain_one(sub, &state);
        samples[sample_count++] = (int64_t)(now_ns() - t0);
    }

    uint64_t elapsed_ns = now_ns() - start_ns;

    qsort(samples, (size_t)sample_count, sizeof(int64_t), compare_int64);

    double mean_ns = 0;
    for (int i = 0; i < sample_count; i++) mean_ns += (double)samples[i];
    if (sample_count > 0) mean_ns /= sample_count;

    printf(
        "{\"client\":\"c\",\"scenario\":\"latency\","
        "\"payload_size\":%d,"
        "\"samples\":%d,"
        "\"ops_per_sec\":%.3f,"
        "\"elapsed_ms\":%.3f,"
        "\"mean_us\":%.4f,"
        "\"median_us\":%.4f,"
        "\"p99_us\":%.4f,"
        "\"p999_us\":%.4f,"
        "\"min_us\":%.4f,"
        "\"max_us\":%.4f}\n",
        cfg->size,
        sample_count,
        elapsed_ns == 0 ? 0 : (double)sample_count * 1e9 / (double)elapsed_ns,
        (double)elapsed_ns / 1e6,
        mean_ns / 1000.0,
        percentile(samples, sample_count, 50) / 1000.0,
        percentile(samples, sample_count, 99) / 1000.0,
        percentile(samples, sample_count, 99.9) / 1000.0,
        sample_count == 0 ? 0 : (double)samples[0] / 1000.0,
        sample_count == 0 ? 0 : (double)samples[sample_count - 1] / 1000.0);

    free(samples);
}

static void run_throughput(
    aeron_publication_t *pub,
    aeron_subscription_t *sub,
    const payload_pool_t *pool,
    const bench_config_t *cfg)
{
    counter_state_t state = { 0 };
    int64_t sequence = 0;

    uint64_t warmup_deadline = now_ns() + (uint64_t)cfg->warmup_seconds * 1000000000ULL;
    while (now_ns() < warmup_deadline)
    {
        uint8_t *message = payload_pool_entry(pool, sequence);
        write_tick(message, cfg->size, sequence++);
        offer_until_accepted(pub, message, cfg->size);
        aeron_subscription_poll(sub, throughput_fragment_handler, &state, 1024);
    }

    state.count = 0;
    state.bid_sum = 0;
    sequence = 0;
    uint64_t start_ns = now_ns();
    uint64_t run_deadline = start_ns + (uint64_t)cfg->time_seconds * 1000000000ULL;

    int64_t sent = 0;
    while (now_ns() < run_deadline)
    {
        for (int i = 0; i < 1000; i++)
        {
            uint8_t *message = payload_pool_entry(pool, sequence);
            write_tick(message, cfg->size, sequence++);
            offer_with_drain(pub, sub, message, cfg->size, &state);
            sent++;
        }
        aeron_subscription_poll(sub, throughput_fragment_handler, &state, 1024);
    }
    uint64_t elapsed_ns = now_ns() - start_ns;
    int64_t received = state.count;

    while (received < sent)
    {
        state.count = 0;
        int n = aeron_subscription_poll(sub, throughput_fragment_handler, &state, 1024);
        received += state.count;
        if (n == 0)
        {
            break;
        }
    }

    fprintf(stderr, "[bench-c] throughput bid_sum=%" PRId64 "\n", state.bid_sum);

    double ops_per_sec = elapsed_ns == 0 ? 0 : (double)sent * 1e9 / (double)elapsed_ns;

    printf(
        "{\"client\":\"c\",\"scenario\":\"throughput\","
        "\"payload_size\":%d,"
        "\"samples\":%" PRId64 ","
        "\"ops_per_sec\":%.3f,"
        "\"bytes_per_sec\":%.3f,"
        "\"received\":%" PRId64 ","
        "\"elapsed_ms\":%.3f}\n",
        cfg->size,
        sent,
        ops_per_sec,
        ops_per_sec * cfg->size,
        received,
        (double)elapsed_ns / 1e6);
}

int main(int argc, char **argv)
{
    bench_config_t cfg;
    parse_args(argc, argv, &cfg);

    fprintf(stderr, "[bench-c] mode=%s size=%d time=%ds warmup=%ds dir=%s\n",
        cfg.mode, cfg.size, cfg.time_seconds, cfg.warmup_seconds, cfg.aeron_dir);

    aeron_context_t *context = NULL;
    aeron_t *aeron = NULL;
    aeron_async_add_publication_t *pub_async = NULL;
    aeron_async_add_subscription_t *sub_async = NULL;
    aeron_publication_t *publication = NULL;
    aeron_subscription_t *subscription = NULL;

    if (aeron_context_init(&context) < 0)
    {
        fprintf(stderr, "aeron_context_init: %s\n", aeron_errmsg());
        return 1;
    }
    if (aeron_context_set_dir(context, cfg.aeron_dir) < 0)
    {
        fprintf(stderr, "aeron_context_set_dir: %s\n", aeron_errmsg());
        return 1;
    }
    if (aeron_init(&aeron, context) < 0)
    {
        fprintf(stderr, "aeron_init: %s\n", aeron_errmsg());
        return 1;
    }
    if (aeron_start(aeron) < 0)
    {
        fprintf(stderr, "aeron_start: %s\n", aeron_errmsg());
        return 1;
    }

    const char *term_length_env = getenv("BENCH_TERM_LENGTH");
    const char *term_length = (term_length_env && *term_length_env) ? term_length_env : "16777216";
    char channel[160];
    snprintf(channel, sizeof(channel), "aeron:ipc?alias=c-bench-%" PRIu64 "|term-length=%s",
        now_ns(), term_length);
    int32_t stream_id = (int32_t)(now_ns() & 0x7FFFFFFF);

    if (aeron_async_add_publication(&pub_async, aeron, channel, stream_id) < 0)
    {
        fprintf(stderr, "aeron_async_add_publication: %s\n", aeron_errmsg());
        return 1;
    }
    while (publication == NULL)
    {
        if (aeron_async_add_publication_poll(&publication, pub_async) < 0)
        {
            fprintf(stderr, "pub poll: %s\n", aeron_errmsg());
            return 1;
        }
        sched_yield();
    }

    if (aeron_async_add_subscription(
        &sub_async, aeron, channel, stream_id, NULL, NULL, NULL, NULL) < 0)
    {
        fprintf(stderr, "aeron_async_add_subscription: %s\n", aeron_errmsg());
        return 1;
    }
    while (subscription == NULL)
    {
        if (aeron_async_add_subscription_poll(&subscription, sub_async) < 0)
        {
            fprintf(stderr, "sub poll: %s\n", aeron_errmsg());
            return 1;
        }
        sched_yield();
    }

    wait_for_connection(publication, subscription);

    payload_pool_t pool;
    if (payload_pool_init(&pool, (size_t)cfg.size) < 0)
    {
        fprintf(stderr, "alloc failed\n");
        return 1;
    }

    if (strcmp(cfg.mode, "latency") == 0)
    {
        run_latency(publication, subscription, &pool, &cfg);
    }
    else if (strcmp(cfg.mode, "throughput") == 0)
    {
        run_throughput(publication, subscription, &pool, &cfg);
    }
    else
    {
        fprintf(stderr, "unknown mode: %s\n", cfg.mode);
        return 1;
    }

    payload_pool_close(&pool);
    aeron_publication_close(publication, NULL, NULL);
    aeron_subscription_close(subscription, NULL, NULL);
    aeron_close(aeron);
    aeron_context_close(context);

    return 0;
}
