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

#include "aeronc.h"

#define BUCKETS 100000
#define BUCKET_NS 100
#define IDLE_EXIT_NS 750000000LL

typedef struct consumer_state_s
{
    int64_t consumed;
    int64_t first_sequence;
    int64_t last_sequence;
    int64_t gaps;
    int64_t price_sum;
    int64_t negatives;
    int64_t max_ns;
    int64_t *counts;
}
consumer_state_t;

static int64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + (int64_t)ts.tv_nsec;
}

static void on_fragment(void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    (void)header;
    consumer_state_t *state = (consumer_state_t *)clientd;

    if (length < 36)
    {
        return;
    }

    uint32_t instrument_id;
    int64_t bid;
    int64_t ask;
    int64_t sequence;
    int64_t published_at;

    memcpy(&instrument_id, buffer + 0, sizeof(instrument_id));
    memcpy(&bid, buffer + 4, sizeof(bid));
    memcpy(&ask, buffer + 12, sizeof(ask));
    memcpy(&sequence, buffer + 20, sizeof(sequence));
    memcpy(&published_at, buffer + 28, sizeof(published_at));

    int64_t latency = now_ns() - published_at;
    if (latency < 0)
    {
        state->negatives++;
    }
    else
    {
        int64_t bucket = latency / BUCKET_NS;
        state->counts[bucket < BUCKETS ? bucket : BUCKETS - 1]++;
        if (latency > state->max_ns)
        {
            state->max_ns = latency;
        }
    }

    if (state->first_sequence < 0)
    {
        state->first_sequence = sequence;
    }
    else if (sequence != state->last_sequence + 1)
    {
        state->gaps++;
    }

    state->last_sequence = sequence;
    state->consumed++;
    state->price_sum += bid + ask + (int64_t)instrument_id;
}

static double percentile(const int64_t *counts, int64_t total, double fraction)
{
    if (total == 0)
    {
        return 0.0;
    }
    int64_t target = (int64_t)(total * fraction);
    if (target < 1)
    {
        target = 1;
    }
    int64_t seen = 0;
    for (int index = 0; index < BUCKETS; index++)
    {
        seen += counts[index];
        if (seen >= target)
        {
            return ((double)index * BUCKET_NS + BUCKET_NS / 2.0) / 1000.0;
        }
    }
    return 0.0;
}

static const char *env_or(const char *name, const char *fallback)
{
    const char *value = getenv(name);
    return (value == NULL || value[0] == '\0') ? fallback : value;
}

int main(void)
{
    const char *aeron_dir = env_or("AERON_DIR", "/tmp/ae_drv");
    const char *channel = env_or("HARNESS_CHANNEL", "aeron:ipc?alias=prices|term-length=67108864");
    const int32_t stream = (int32_t)atoi(env_or("HARNESS_STREAM", "9001"));
    const int duration_s = atoi(env_or("HARNESS_DURATION_S", "30")) + 10;
    const char *label = env_or("HARNESS_LABEL", "c");

    consumer_state_t state;
    memset(&state, 0, sizeof(state));
    state.first_sequence = -1;
    state.last_sequence = -1;
    state.counts = (int64_t *)calloc(BUCKETS, sizeof(int64_t));

    aeron_context_t *context = NULL;
    aeron_t *aeron = NULL;
    aeron_async_add_subscription_t *async = NULL;
    aeron_subscription_t *subscription = NULL;
    aeron_fragment_assembler_t *assembler = NULL;

    if (aeron_context_init(&context) < 0 ||
        aeron_context_set_dir(context, aeron_dir) < 0 ||
        aeron_init(&aeron, context) < 0 ||
        aeron_start(aeron) < 0)
    {
        fprintf(stderr, "[consumer-%s] init: %s\n", label, aeron_errmsg());
        return 1;
    }

    if (aeron_async_add_subscription(&async, aeron, channel, stream, NULL, NULL, NULL, NULL) < 0)
    {
        fprintf(stderr, "[consumer-%s] add subscription: %s\n", label, aeron_errmsg());
        return 1;
    }
    while (subscription == NULL)
    {
        if (aeron_async_add_subscription_poll(&subscription, async) < 0)
        {
            fprintf(stderr, "[consumer-%s] subscription poll: %s\n", label, aeron_errmsg());
            return 1;
        }
        sched_yield();
    }

    printf("READY\n");
    fflush(stdout);
    fprintf(stderr, "[consumer-%s] subscribed, awaiting image\n", label);

    while (!aeron_subscription_is_connected(subscription))
    {
        sched_yield();
    }
    fprintf(stderr, "[consumer-%s] image attached\n", label);

    if (aeron_fragment_assembler_create(&assembler, on_fragment, &state) < 0)
    {
        fprintf(stderr, "[consumer-%s] assembler: %s\n", label, aeron_errmsg());
        return 1;
    }

    const int64_t deadline = now_ns() + (int64_t)duration_s * 1000000000LL;
    int64_t last_message_at = now_ns();

    while (1)
    {
        const int64_t current = now_ns();
        if (current >= deadline)
        {
            break;
        }
        if (state.consumed > 0 && current - last_message_at > IDLE_EXIT_NS)
        {
            break;
        }
        if (aeron_subscription_poll(subscription, aeron_fragment_assembler_handler, assembler, 1024) > 0)
        {
            last_message_at = current;
        }
    }

    int64_t total = 0;
    double weighted = 0.0;
    for (int index = 0; index < BUCKETS; index++)
    {
        total += state.counts[index];
        weighted += (double)state.counts[index] * ((double)index * BUCKET_NS + BUCKET_NS / 2.0);
    }
    const double mean_us = total > 0 ? weighted / (double)total / 1000.0 : 0.0;

    fprintf(stderr, "[consumer-%s] price_sum=%" PRId64 "\n", label, state.price_sum);

    printf(
        "{\"role\":\"consumer\",\"client\":\"%s\",\"consumed\":%" PRId64 ","
        "\"first_sequence\":%" PRId64 ",\"last_sequence\":%" PRId64 ","
        "\"sequence_span\":%" PRId64 ",\"gaps\":%" PRId64 ","
        "\"p50_us\":%.3f,\"p90_us\":%.3f,\"p99_us\":%.3f,\"p999_us\":%.3f,"
        "\"max_us\":%.3f,\"mean_us\":%.3f,\"negative_latencies\":%" PRId64 "}\n",
        label,
        state.consumed,
        state.first_sequence,
        state.last_sequence,
        state.last_sequence - state.first_sequence + 1,
        state.gaps,
        percentile(state.counts, total, 0.5),
        percentile(state.counts, total, 0.9),
        percentile(state.counts, total, 0.99),
        percentile(state.counts, total, 0.999),
        (double)state.max_ns / 1000.0,
        mean_us,
        state.negatives);
    fflush(stdout);

    aeron_subscription_close(subscription, NULL, NULL);
    aeron_close(aeron);
    aeron_context_close(context);
    free(state.counts);
    return 0;
}
