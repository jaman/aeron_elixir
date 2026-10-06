#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cinttypes>
#include <chrono>
#include <string>
#include <vector>
#include <algorithm>
#include <thread>
#include <sched.h>

#include "Aeron.h"
#include "payload_pool.hpp"

using namespace aeron;

struct Config
{
    std::string mode = "latency";
    int size = 32;
    int timeSeconds = 10;
    int warmupSeconds = 3;
    std::string aeronDir;
};

static Config parseArgs(int argc, char **argv)
{
    Config cfg;
    if (const char *env = std::getenv("AERON_DIR"))
    {
        cfg.aeronDir = env;
    }
    else
    {
        cfg.aeronDir = "/tmp/ae_drv";
    }

    for (int i = 1; i < argc; i++)
    {
        std::string arg = argv[i];
        auto eq = arg.find('=');
        if (eq == std::string::npos)
        {
            continue;
        }
        std::string key = arg.substr(0, eq);
        std::string val = arg.substr(eq + 1);

        if (key == "--mode") cfg.mode = val;
        else if (key == "--size") cfg.size = std::atoi(val.c_str());
        else if (key == "--time") cfg.timeSeconds = std::atoi(val.c_str());
        else if (key == "--warmup") cfg.warmupSeconds = std::atoi(val.c_str());
        else if (key == "--aeron-dir") cfg.aeronDir = val;
    }
    return cfg;
}

static std::int64_t nowNs()
{
    using namespace std::chrono;
    return duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count();
}

static void writeTick(std::uint8_t *buf, int len, std::int64_t sequence)
{
    if (len < 28)
    {
        return;
    }
    std::uint32_t instrumentId = static_cast<std::uint32_t>(sequence % 100);
    std::int64_t bid = 100000 + (sequence % 1000);
    std::int64_t ask = bid + 10;
    std::memcpy(buf + 0, &instrumentId, sizeof(instrumentId));
    std::memcpy(buf + 4, &bid, sizeof(bid));
    std::memcpy(buf + 12, &ask, sizeof(ask));
    std::memcpy(buf + 20, &sequence, sizeof(sequence));
}

struct CounterState
{
    std::atomic<std::int64_t> count{ 0 };
};

static void offerUntilAccepted(Publication &pub, const std::uint8_t *buf, int len)
{
    while (true)
    {
        std::int64_t result = pub.offer(buf, static_cast<std::size_t>(len));
        if (result > 0)
        {
            return;
        }
        if (result == PUBLICATION_CLOSED || result == MAX_POSITION_EXCEEDED)
        {
            std::fprintf(stderr, "publication closed: %" PRId64 "\n", result);
            std::exit(1);
        }
    }
}

template <typename Handler>
static void offerWithDrain(
    Publication &pub, Subscription &sub, const std::uint8_t *buf, int len, Handler &&handler)
{
    while (true)
    {
        std::int64_t result = pub.offer(buf, static_cast<std::size_t>(len));
        if (result > 0)
        {
            return;
        }
        if (result == PUBLICATION_CLOSED || result == MAX_POSITION_EXCEEDED)
        {
            std::fprintf(stderr, "publication closed: %" PRId64 "\n", result);
            std::exit(1);
        }
        sub.poll(handler, 1024);
    }
}

static void waitForConnection(Publication &pub, Subscription &sub)
{
    std::int64_t deadline = nowNs() + 10LL * 1000000000LL;
    while (!(pub.isConnected() && sub.isConnected()))
    {
        if (nowNs() > deadline)
        {
            std::fprintf(stderr, "connection timeout\n");
            std::exit(1);
        }
        std::this_thread::yield();
    }
}

static void drainOne(Subscription &sub, CounterState &state,
                     fragment_handler_t handler)
{
    state.count = 0;
    while (state.count.load() == 0)
    {
        sub.poll(handler, 1);
    }
}

static int drainAll(Subscription &sub, fragment_handler_t handler, int maxPolls)
{
    int total = 0;
    for (int i = 0; i < maxPolls; i++)
    {
        int n = sub.poll(handler, 1024);
        total += n;
        if (n == 0)
        {
            break;
        }
    }
    return total;
}

static double percentile(const std::vector<std::int64_t> &sorted, double pct)
{
    if (sorted.empty())
    {
        return 0.0;
    }
    std::size_t idx = static_cast<std::size_t>((pct / 100.0) * sorted.size());
    if (idx >= sorted.size()) idx = sorted.size() - 1;
    return static_cast<double>(sorted[idx]);
}

static void runLatency(
    Publication &pub, Subscription &sub,
    const PayloadPool &pool,
    const Config &cfg)
{
    CounterState state;
    std::int64_t sequence = 0;
    auto handler = [&](const AtomicBuffer &buffer, util::index_t offset,
                       util::index_t length, const Header &header)
    {
        (void)buffer; (void)offset; (void)length; (void)header;
        state.count++;
    };

    std::int64_t warmupDeadline = nowNs() + cfg.warmupSeconds * 1000000000LL;
    while (nowNs() < warmupDeadline)
    {
        state.count = 0;
        std::uint8_t *message = pool.entry(sequence);
        writeTick(message, cfg.size, sequence++);
        offerUntilAccepted(pub, message, cfg.size);
        drainOne(sub, state, handler);
    }

    std::vector<std::int64_t> samples;
    samples.reserve(static_cast<std::size_t>(cfg.timeSeconds) * 10000000);

    std::int64_t startNs = nowNs();
    std::int64_t runDeadline = startNs + cfg.timeSeconds * 1000000000LL;

    while (nowNs() < runDeadline)
    {
        state.count = 0;
        std::int64_t t0 = nowNs();
        std::uint8_t *message = pool.entry(sequence);
        writeTick(message, cfg.size, sequence++);
        offerUntilAccepted(pub, message, cfg.size);
        drainOne(sub, state, handler);
        samples.push_back(nowNs() - t0);
    }
    std::int64_t elapsedNs = nowNs() - startNs;

    std::sort(samples.begin(), samples.end());

    double meanNs = 0;
    for (auto s : samples) meanNs += static_cast<double>(s);
    if (!samples.empty()) meanNs /= samples.size();

    double opsPerSec = elapsedNs == 0 ? 0 :
        static_cast<double>(samples.size()) * 1e9 / static_cast<double>(elapsedNs);

    std::printf(
        "{\"client\":\"cpp\",\"scenario\":\"latency\","
        "\"payload_size\":%d,"
        "\"samples\":%zu,"
        "\"ops_per_sec\":%.3f,"
        "\"elapsed_ms\":%.3f,"
        "\"mean_us\":%.4f,"
        "\"median_us\":%.4f,"
        "\"p99_us\":%.4f,"
        "\"p999_us\":%.4f,"
        "\"min_us\":%.4f,"
        "\"max_us\":%.4f}\n",
        cfg.size,
        samples.size(),
        opsPerSec,
        static_cast<double>(elapsedNs) / 1e6,
        meanNs / 1000.0,
        percentile(samples, 50) / 1000.0,
        percentile(samples, 99) / 1000.0,
        percentile(samples, 99.9) / 1000.0,
        samples.empty() ? 0 : static_cast<double>(samples.front()) / 1000.0,
        samples.empty() ? 0 : static_cast<double>(samples.back()) / 1000.0);
}

static void runThroughput(
    Publication &pub, Subscription &sub,
    const PayloadPool &pool,
    const Config &cfg)
{
    std::int64_t totalReceived = 0;
    volatile std::int64_t bidSum = 0;
    auto handler = [&](const AtomicBuffer &buffer, util::index_t offset,
                       util::index_t length, const Header &header)
    {
        (void)header;
        totalReceived++;
        if (length >= 28)
        {
            std::uint32_t instrumentId;
            std::int64_t bid;
            std::int64_t ask;
            std::int64_t sequence;
            const std::uint8_t *src = buffer.buffer() + offset;
            std::memcpy(&instrumentId, src + 0, sizeof(instrumentId));
            std::memcpy(&bid, src + 4, sizeof(bid));
            std::memcpy(&ask, src + 12, sizeof(ask));
            std::memcpy(&sequence, src + 20, sizeof(sequence));
            bidSum = bidSum + bid + static_cast<std::int64_t>(instrumentId) + ask + sequence;
        }
    };

    std::int64_t sequence = 0;
    std::int64_t warmupDeadline = nowNs() + cfg.warmupSeconds * 1000000000LL;
    while (nowNs() < warmupDeadline)
    {
        std::uint8_t *message = pool.entry(sequence);
        writeTick(message, cfg.size, sequence++);
        offerUntilAccepted(pub, message, cfg.size);
        drainAll(sub, handler, 4);
    }

    sequence = 0;
    std::int64_t startNs = nowNs();
    std::int64_t runDeadline = startNs + cfg.timeSeconds * 1000000000LL;

    std::int64_t sent = 0;
    while (nowNs() < runDeadline)
    {
        for (int i = 0; i < 1000; i++)
        {
            std::uint8_t *message = pool.entry(sequence);
            writeTick(message, cfg.size, sequence++);
            offerWithDrain(pub, sub, message, cfg.size, handler);
            sent++;
        }
        drainAll(sub, handler, 4);
    }
    std::int64_t elapsedNs = nowNs() - startNs;

    while (totalReceived < sent)
    {
        int drained = drainAll(sub, handler, 8);
        if (drained == 0)
        {
            break;
        }
    }

    double opsPerSec = elapsedNs == 0 ? 0 :
        static_cast<double>(sent) * 1e9 / static_cast<double>(elapsedNs);

    std::printf(
        "{\"client\":\"cpp\",\"scenario\":\"throughput\","
        "\"payload_size\":%d,"
        "\"samples\":%" PRId64 ","
        "\"ops_per_sec\":%.3f,"
        "\"bytes_per_sec\":%.3f,"
        "\"received\":%" PRId64 ","
        "\"elapsed_ms\":%.3f}\n",
        cfg.size,
        sent,
        opsPerSec,
        opsPerSec * cfg.size,
        totalReceived,
        static_cast<double>(elapsedNs) / 1e6);

    std::fprintf(stderr, "[bench-cpp] throughput bid_sum=%" PRId64 "\n",
        static_cast<std::int64_t>(bidSum));
}

int main(int argc, char **argv)
{
    Config cfg = parseArgs(argc, argv);

    std::fprintf(stderr, "[bench-cpp] mode=%s size=%d time=%ds warmup=%ds dir=%s\n",
        cfg.mode.c_str(), cfg.size, cfg.timeSeconds, cfg.warmupSeconds, cfg.aeronDir.c_str());

    Context context;
    context.aeronDir(cfg.aeronDir);

    try
    {
        std::shared_ptr<Aeron> aeron = Aeron::connect(context);

        const char *termLengthEnv = std::getenv("BENCH_TERM_LENGTH");
        const char *termLength = (termLengthEnv && *termLengthEnv) ? termLengthEnv : "16777216";
        char channel[160];
        std::int64_t ts = nowNs();
        std::snprintf(channel, sizeof(channel), "aeron:ipc?alias=cpp-bench-%" PRId64 "|term-length=%s",
            ts, termLength);
        std::int32_t streamId = static_cast<std::int32_t>(ts & 0x7FFFFFFF);

        AsyncAddPublication *pubAsync = aeron->addPublicationAsync(channel, streamId);
        AsyncAddSubscription *subAsync = aeron->addSubscriptionAsync(channel, streamId);

        std::shared_ptr<Publication> pub;
        std::shared_ptr<Subscription> sub;

        while (!pub)
        {
            pub = aeron->findPublication(pubAsync);
            std::this_thread::yield();
        }
        while (!sub)
        {
            sub = aeron->findSubscription(subAsync);
            std::this_thread::yield();
        }

        waitForConnection(*pub, *sub);

        PayloadPool pool(static_cast<std::size_t>(cfg.size));

        if (cfg.mode == "latency")
        {
            runLatency(*pub, *sub, pool, cfg);
        }
        else if (cfg.mode == "throughput")
        {
            runThroughput(*pub, *sub, pool, cfg);
        }
        else
        {
            std::fprintf(stderr, "unknown mode: %s\n", cfg.mode.c_str());
            return 1;
        }

        pub.reset();
        sub.reset();
    }
    catch (const std::exception &e)
    {
        std::fprintf(stderr, "aeron error: %s\n", e.what());
        return 1;
    }

    return 0;
}
