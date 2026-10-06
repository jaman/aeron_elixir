#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cinttypes>
#include <string>
#include <thread>
#include <vector>

#include "Aeron.h"
#include "payload_pool.hpp"

using namespace aeron;

struct Config
{
    std::string mode = "ping";
    std::string aeronDir;
    std::int32_t pingStream = 7100;
    std::int32_t pongStream = 7101;
    int length = 32;
    std::int64_t warmup = 100000;
    std::int64_t messages = 1000000;
    std::int64_t startAtNs = 0;
};

static constexpr std::int64_t startSpinNs = 2000000;

static std::atomic<bool> running{true};

static void handleSignal(int)
{
    running = false;
}

static std::int64_t nowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

static std::int64_t realtimeNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

static void awaitStart(std::int64_t startAtNs)
{
    const std::int64_t sleepNs = startAtNs - realtimeNs() - startSpinNs;
    if (sleepNs > 0)
    {
        std::this_thread::sleep_for(std::chrono::nanoseconds(sleepNs));
    }
    while (realtimeNs() < startAtNs)
    {
    }
}

static int envInt(const char *name, int fallback)
{
    const char *value = std::getenv(name);
    return (value != nullptr && *value != '\0') ? std::atoi(value) : fallback;
}

static std::int64_t envInt64(const char *name, std::int64_t fallback)
{
    const char *value = std::getenv(name);
    return (value != nullptr && *value != '\0') ? std::strtoll(value, nullptr, 10) : fallback;
}

static Config parseArgs(int argc, char **argv)
{
    Config cfg;
    const char *dir = std::getenv("AERON_DIR");
    cfg.aeronDir = (dir != nullptr && *dir != '\0') ? dir : "/tmp/ae_drv";

    const int base = envInt("RPC_STREAM_BASE", 7100);
    const int pairIndex = envInt("RPC_PAIR_INDEX", 0);
    cfg.pingStream = static_cast<std::int32_t>(base + pairIndex * 2);
    cfg.pongStream = static_cast<std::int32_t>(base + pairIndex * 2 + 1);
    cfg.length = envInt("RPC_MESSAGE_LENGTH", 32);
    cfg.warmup = envInt64("RPC_WARMUP_MESSAGES", 100000);
    cfg.messages = envInt64("RPC_MESSAGES", 1000000);
    cfg.startAtNs = envInt64("RPC_START_AT_NS", 0);

    for (int i = 1; i < argc; i++)
    {
        const std::string arg = argv[i];
        if (arg.rfind("--mode=", 0) == 0)
        {
            cfg.mode = arg.substr(7);
        }
    }
    return cfg;
}

static void writeTick(std::uint8_t *buf, int len, std::int64_t sequence)
{
    if (len < 28)
    {
        return;
    }
    const std::uint32_t instrumentId = static_cast<std::uint32_t>(sequence % 100);
    const std::int64_t bid = 100000 + (sequence % 1000);
    const std::int64_t ask = bid + 10;
    std::memcpy(buf + 0, &instrumentId, sizeof(instrumentId));
    std::memcpy(buf + 4, &bid, sizeof(bid));
    std::memcpy(buf + 12, &ask, sizeof(ask));
    std::memcpy(buf + 20, &sequence, sizeof(sequence));
}

static std::int64_t readTick(const std::uint8_t *buf, std::int32_t length)
{
    if (length < 28)
    {
        return 0;
    }
    std::uint32_t instrumentId;
    std::int64_t bid;
    std::int64_t ask;
    std::int64_t sequence;
    std::memcpy(&instrumentId, buf + 0, sizeof(instrumentId));
    std::memcpy(&bid, buf + 4, sizeof(bid));
    std::memcpy(&ask, buf + 12, sizeof(ask));
    std::memcpy(&sequence, buf + 20, sizeof(sequence));
    return bid + static_cast<std::int64_t>(instrumentId) + ask + sequence;
}

static double percentileUs(const std::vector<std::int64_t> &sorted, double pct)
{
    auto index = static_cast<std::size_t>(pct / 100.0 * static_cast<double>(sorted.size()));
    if (index >= sorted.size())
    {
        index = sorted.size() - 1;
    }
    return static_cast<double>(sorted[index]) / 1000.0;
}

static std::shared_ptr<Publication> addPublication(std::shared_ptr<Aeron> aeron, std::int32_t streamId)
{
    AsyncAddPublication *async = aeron->addPublicationAsync("aeron:ipc", streamId);
    std::shared_ptr<Publication> publication;
    while (!publication)
    {
        publication = aeron->findPublication(async);
        std::this_thread::yield();
    }
    return publication;
}

static std::shared_ptr<Subscription> addSubscription(std::shared_ptr<Aeron> aeron, std::int32_t streamId)
{
    AsyncAddSubscription *async = aeron->addSubscriptionAsync("aeron:ipc", streamId);
    std::shared_ptr<Subscription> subscription;
    while (!subscription)
    {
        subscription = aeron->findSubscription(async);
        std::this_thread::yield();
    }
    return subscription;
}

static bool awaitConnected(std::shared_ptr<Publication> pub, std::shared_ptr<Subscription> sub)
{
    const std::int64_t deadline = nowNs() + 30LL * 1000000000LL;
    while (!(pub->isConnected() && sub->isConnected()))
    {
        if (nowNs() > deadline)
        {
            std::fprintf(stderr, "[bench-cpp-rpc] connection timeout\n");
            return false;
        }
        std::this_thread::yield();
    }
    return true;
}

static int runPing(const Config &cfg, std::shared_ptr<Aeron> aeron)
{
    auto publication = addPublication(aeron, cfg.pingStream);
    auto subscription = addSubscription(aeron, cfg.pongStream);
    if (!awaitConnected(publication, subscription))
    {
        return 1;
    }

    PayloadPool pool(static_cast<std::size_t>(cfg.length));
    std::vector<std::int64_t> samples(static_cast<std::size_t>(cfg.messages));

    std::int64_t received = 0;
    std::int64_t tickSum = 0;

    auto handler = [&](const AtomicBuffer &buffer, util::index_t offset,
                       util::index_t length, const Header &header)
    {
        (void)header;
        received++;
        tickSum += readTick(buffer.buffer() + offset, length);
    };

    auto roundTrip = [&](std::int64_t sequence)
    {
        std::uint8_t *message = pool.entry(sequence);
        writeTick(message, cfg.length, sequence);
        while (publication->offer(message, static_cast<std::size_t>(cfg.length)) < 0)
        {
        }
        received = 0;
        while (received == 0)
        {
            subscription->poll(handler, 1);
        }
    };

    std::int64_t sequence = 0;
    for (std::int64_t i = 0; i < cfg.warmup; i++)
    {
        roundTrip(sequence++);
    }

    awaitStart(cfg.startAtNs);
    const std::int64_t startedAtNs = realtimeNs();
    const std::int64_t startNs = nowNs();
    for (std::int64_t i = 0; i < cfg.messages; i++)
    {
        const std::int64_t t0 = nowNs();
        roundTrip(sequence++);
        samples[static_cast<std::size_t>(i)] = nowNs() - t0;
    }
    const std::int64_t elapsedNs = nowNs() - startNs;
    const std::int64_t finishedAtNs = realtimeNs();

    double meanNs = 0;
    for (const auto sample : samples)
    {
        meanNs += static_cast<double>(sample);
    }
    meanNs /= static_cast<double>(cfg.messages);

    std::sort(samples.begin(), samples.end());

    std::fprintf(stderr, "[bench-cpp-rpc] tick_sum=%" PRId64 "\n", tickSum);

    std::printf(
        "{\"client\":\"cpp\",\"scenario\":\"rpc\","
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
        cfg.length,
        cfg.messages,
        static_cast<double>(elapsedNs) / 1e6,
        meanNs / 1000.0,
        percentileUs(samples, 50.0),
        percentileUs(samples, 90.0),
        percentileUs(samples, 99.0),
        percentileUs(samples, 99.9),
        static_cast<double>(samples.back()) / 1000.0,
        static_cast<double>(cfg.messages) * 1e9 / static_cast<double>(elapsedNs),
        startedAtNs,
        finishedAtNs);
    std::fflush(stdout);

    return 0;
}

static int runPong(const Config &cfg, std::shared_ptr<Aeron> aeron)
{
    auto subscription = addSubscription(aeron, cfg.pingStream);
    auto publication = addPublication(aeron, cfg.pongStream);

    std::printf("READY\n");
    std::fflush(stdout);

    if (!awaitConnected(publication, subscription))
    {
        return 1;
    }

    std::int64_t received = 0;
    std::int64_t tickSum = 0;

    auto handler = [&](const AtomicBuffer &buffer, util::index_t offset,
                       util::index_t length, const Header &header)
    {
        (void)header;
        received++;
        const std::uint8_t *src = buffer.buffer() + offset;
        tickSum += readTick(src, length);
        while (running && publication->offer(src, static_cast<std::size_t>(length)) < 0)
        {
        }
    };

    while (running)
    {
        subscription->poll(handler, 1);
    }

    std::fprintf(stderr, "[bench-cpp-rpc] pong tick_sum=%" PRId64 " received=%" PRId64 "\n",
        tickSum, received);
    return 0;
}

int main(int argc, char **argv)
{
    const Config cfg = parseArgs(argc, argv);

    std::signal(SIGTERM, handleSignal);
    std::signal(SIGINT, handleSignal);

    std::fprintf(stderr, "[bench-cpp-rpc] mode=%s ping=%d pong=%d length=%d dir=%s\n",
        cfg.mode.c_str(), cfg.pingStream, cfg.pongStream, cfg.length, cfg.aeronDir.c_str());

    try
    {
        Context context;
        context.aeronDir(cfg.aeronDir);
        std::shared_ptr<Aeron> aeron = Aeron::connect(context);

        return cfg.mode == "pong" ? runPong(cfg, aeron) : runPing(cfg, aeron);
    }
    catch (const std::exception &e)
    {
        std::fprintf(stderr, "[bench-cpp-rpc] %s\n", e.what());
        return 1;
    }
}
