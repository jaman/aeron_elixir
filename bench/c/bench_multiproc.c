#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <inttypes.h>
#include <sched.h>
#include <pthread.h>

#include "aeronc.h"
#include "payload_pool.h"

typedef struct bench_config_s
{
    int workers;
    int size;
    int time_seconds;
    int warmup_seconds;
    const char *aeron_dir;
    const char *term_length;
}
bench_config_t;

typedef struct counter_state_s
{
    int64_t count;
    volatile int64_t bid_sum;
}
counter_state_t;

typedef struct worker_result_s
{
    int index;
    const bench_config_t *cfg;
    int64_t sent;
    int64_t received;
    uint64_t elapsed_ns;
    int failed;
}
worker_result_t;

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
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

static void parse_args(int argc, char **argv, bench_config_t *cfg)
{
    cfg->workers = 4;
    cfg->size = 256;
    cfg->time_seconds = 5;
    cfg->warmup_seconds = 2;
    cfg->aeron_dir = getenv("AERON_DIR");
    if (cfg->aeron_dir == NULL)
    {
        cfg->aeron_dir = "/tmp/ae_drv";
    }
    const char *term_length_env = getenv("BENCH_TERM_LENGTH");
    cfg->term_length = (term_length_env && *term_length_env) ? term_length_env : "16777216";

    for (int i = 1; i < argc; i++)
    {
        const char *eq = strchr(argv[i], '=');
        if (eq == NULL)
        {
            continue;
        }
        ptrdiff_t keylen = eq - argv[i];
        const char *val = eq + 1;
        if (keylen == 9 && strncmp(argv[i], "--workers", 9) == 0)
        {
            cfg->workers = atoi(val);
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
    }
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

static int offer_until_accepted(aeron_publication_t *pub, const uint8_t *buf, int len)
{
    while (1)
    {
        int64_t result = aeron_publication_offer(pub, buf, (size_t)len, NULL, NULL);
        if (result > 0)
        {
            return 0;
        }
        if (result == AERON_PUBLICATION_CLOSED || result == AERON_PUBLICATION_MAX_POSITION_EXCEEDED)
        {
            return -1;
        }
    }
}

static int offer_with_drain(
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
            return 0;
        }
        if (result == AERON_PUBLICATION_CLOSED || result == AERON_PUBLICATION_MAX_POSITION_EXCEEDED)
        {
            return -1;
        }
        aeron_subscription_poll(sub, throughput_fragment_handler, state, 1024);
    }
}

static int wait_for_connection(aeron_publication_t *pub, aeron_subscription_t *sub)
{
    uint64_t deadline = now_ns() + 10ULL * 1000000000ULL;
    while (!(aeron_publication_is_connected(pub) && aeron_subscription_is_connected(sub)))
    {
        if (now_ns() > deadline)
        {
            return -1;
        }
        sched_yield();
    }
    return 0;
}

static void *worker_main(void *arg)
{
    worker_result_t *result = (worker_result_t *)arg;
    const bench_config_t *cfg = result->cfg;
    result->failed = 1;

    aeron_context_t *context = NULL;
    aeron_t *aeron = NULL;
    aeron_async_add_publication_t *pub_async = NULL;
    aeron_async_add_subscription_t *sub_async = NULL;
    aeron_publication_t *publication = NULL;
    aeron_subscription_t *subscription = NULL;

    if (aeron_context_init(&context) < 0 ||
        aeron_context_set_dir(context, cfg->aeron_dir) < 0 ||
        aeron_init(&aeron, context) < 0 ||
        aeron_start(aeron) < 0)
    {
        fprintf(stderr, "[bench-c-mp] worker %d client init: %s\n", result->index, aeron_errmsg());
        return NULL;
    }

    char channel[192];
    snprintf(channel, sizeof(channel), "aeron:ipc?alias=c-mp-%d-%" PRIu64 "|term-length=%s",
        result->index, now_ns(), cfg->term_length);
    int32_t stream_id = (int32_t)((now_ns() + (uint64_t)result->index) & 0x7FFFFFFF);

    if (aeron_async_add_publication(&pub_async, aeron, channel, stream_id) < 0)
    {
        fprintf(stderr, "[bench-c-mp] add publication: %s\n", aeron_errmsg());
        return NULL;
    }
    while (publication == NULL)
    {
        if (aeron_async_add_publication_poll(&publication, pub_async) < 0)
        {
            fprintf(stderr, "[bench-c-mp] pub poll: %s\n", aeron_errmsg());
            return NULL;
        }
        sched_yield();
    }

    if (aeron_async_add_subscription(&sub_async, aeron, channel, stream_id, NULL, NULL, NULL, NULL) < 0)
    {
        fprintf(stderr, "[bench-c-mp] add subscription: %s\n", aeron_errmsg());
        return NULL;
    }
    while (subscription == NULL)
    {
        if (aeron_async_add_subscription_poll(&subscription, sub_async) < 0)
        {
            fprintf(stderr, "[bench-c-mp] sub poll: %s\n", aeron_errmsg());
            return NULL;
        }
        sched_yield();
    }

    if (wait_for_connection(publication, subscription) < 0)
    {
        fprintf(stderr, "[bench-c-mp] worker %d connection timeout\n", result->index);
        return NULL;
    }

    payload_pool_t pool;
    if (payload_pool_init(&pool, (size_t)cfg->size) < 0)
    {
        return NULL;
    }

    counter_state_t state = { 0 };
    int64_t sequence = 0;

    uint64_t warmup_deadline = now_ns() + (uint64_t)cfg->warmup_seconds * 1000000000ULL;
    while (now_ns() < warmup_deadline)
    {
        uint8_t *message = payload_pool_entry(&pool, sequence);
        write_tick(message, cfg->size, sequence++);
        if (offer_until_accepted(publication, message, cfg->size) < 0)
        {
            return NULL;
        }
        aeron_subscription_poll(subscription, throughput_fragment_handler, &state, 1024);
    }
    while (aeron_subscription_poll(subscription, throughput_fragment_handler, &state, 1024) > 0)
    {
    }

    state.count = 0;
    state.bid_sum = 0;
    sequence = 0;
    int64_t sent = 0;
    uint64_t start_ns = now_ns();
    uint64_t run_deadline = start_ns + (uint64_t)cfg->time_seconds * 1000000000ULL;

    while (now_ns() < run_deadline)
    {
        for (int i = 0; i < 1000; i++)
        {
            uint8_t *message = payload_pool_entry(&pool, sequence);
            write_tick(message, cfg->size, sequence++);
            if (offer_with_drain(publication, subscription, message, cfg->size, &state) < 0)
            {
                return NULL;
            }
            sent++;
        }
        aeron_subscription_poll(subscription, throughput_fragment_handler, &state, 1024);
    }
    uint64_t elapsed_ns = now_ns() - start_ns;

    while (state.count < sent)
    {
        if (aeron_subscription_poll(subscription, throughput_fragment_handler, &state, 1024) == 0)
        {
            break;
        }
    }

    result->sent = sent;
    result->received = state.count;
    result->elapsed_ns = elapsed_ns;
    result->failed = 0;

    payload_pool_close(&pool);
    aeron_publication_close(publication, NULL, NULL);
    aeron_subscription_close(subscription, NULL, NULL);
    aeron_close(aeron);
    aeron_context_close(context);
    return NULL;
}

int main(int argc, char **argv)
{
    bench_config_t cfg;
    parse_args(argc, argv, &cfg);

    fprintf(stderr, "[bench-c-mp] workers=%d size=%d time=%ds warmup=%ds dir=%s\n",
        cfg.workers, cfg.size, cfg.time_seconds, cfg.warmup_seconds, cfg.aeron_dir);

    pthread_t *threads = (pthread_t *)calloc((size_t)cfg.workers, sizeof(pthread_t));
    worker_result_t *results = (worker_result_t *)calloc((size_t)cfg.workers, sizeof(worker_result_t));

    for (int i = 0; i < cfg.workers; i++)
    {
        results[i].index = i;
        results[i].cfg = &cfg;
        if (pthread_create(&threads[i], NULL, worker_main, &results[i]) != 0)
        {
            fprintf(stderr, "[bench-c-mp] pthread_create failed\n");
            return 1;
        }
    }

    int64_t total_sent = 0;
    int64_t total_received = 0;
    uint64_t max_elapsed = 0;
    int failed = 0;

    for (int i = 0; i < cfg.workers; i++)
    {
        pthread_join(threads[i], NULL);
        if (results[i].failed)
        {
            failed = 1;
            continue;
        }
        total_sent += results[i].sent;
        total_received += results[i].received;
        if (results[i].elapsed_ns > max_elapsed)
        {
            max_elapsed = results[i].elapsed_ns;
        }
    }

    if (failed)
    {
        fprintf(stderr, "[bench-c-mp] a worker failed\n");
        return 1;
    }

    double ops_per_sec = max_elapsed == 0 ? 0 : (double)total_sent * 1e9 / (double)max_elapsed;

    printf(
        "{\"client\":\"c\",\"scenario\":\"throughput\","
        "\"payload_size\":%d,"
        "\"workers\":%d,"
        "\"samples\":%" PRId64 ","
        "\"received\":%" PRId64 ","
        "\"ops_per_sec\":%.3f,"
        "\"bytes_per_sec\":%.3f,"
        "\"elapsed_ms\":%.3f}\n",
        cfg.size,
        cfg.workers,
        total_sent,
        total_received,
        ops_per_sec,
        ops_per_sec * cfg.size,
        (double)max_elapsed / 1e6);

    free(threads);
    free(results);
    return 0;
}
