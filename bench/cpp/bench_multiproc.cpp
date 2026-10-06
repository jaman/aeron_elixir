#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cinttypes>
#include <chrono>
#include <string>
#include <vector>
#include <thread>
#include <atomic>

#include "Aeron.h"
#include "payload_pool.hpp"

using namespace aeron;

struct Config
{
    int workers = 4;
    int size = 32;
    int timeSeconds = 5;
    int warmupSeconds = 2;
    std::string aeronDir;
    std::string termLength;
};

struct WorkerResult
{
    int index = 0;
    std::int64_t sent = 0;
    std::int64_t received = 0;
    std::int64_t elapsedNs = 0;
    std::int64_t bidSum = 0;
    bool failed = true;
};

static Config parseArgs(int argc, char **argv)
{
    Config cfg;
    const char *dir = std::getenv("AERON_DIR");
    cfg.aeronDir = dir ? dir : "/tmp/ae_drv";
    const char *tl = std::getenv("BENCH_TERM_LENGTH");
    cfg.termLength = (tl && *tl) ? tl : "16777216";

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

        if (key == "--workers") cfg.workers = std::atoi(val.c_str());
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

static void workerMain(const Config &cfg, WorkerResult &result)
{
    try
    {
        Context context;
        context.aeronDir(cfg.aeronDir);
        std::shared_ptr<Aeron> aeron = Aeron::connect(context);

        char channel[192];
        std::int64_t ts = nowNs();
        std::snprintf(channel, sizeof(channel), "aeron:ipc?alias=cpp-mp-%d-%" PRId64 "|term-length=%s",
            result.index, ts, cfg.termLength.c_str());
        std::int32_t streamId = static_cast<std::int32_t>((ts + result.index) & 0x7FFFFFFF);

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

        std::int64_t deadline = nowNs() + 10LL * 1000000000LL;
        while (!(pub->isConnected() && sub->isConnected()))
        {
            if (nowNs() > deadline)
            {
                std::fprintf(stderr, "[bench-cpp-mp] worker %d connection timeout\n", result.index);
                return;
            }
            std::this_thread::yield();
        }

        PayloadPool pool(static_cast<std::size_t>(cfg.size));
        std::int64_t received = 0;
        std::int64_t bidSum = 0;

        auto handler = [&](const AtomicBuffer &buffer, util::index_t offset,
                           util::index_t length, const Header &header)
        {
            (void)header;
            received++;
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
                bidSum += bid + static_cast<std::int64_t>(instrumentId) + ask + sequence;
            }
        };

        auto offerWithDrain = [&](std::int64_t sequence) -> bool
        {
            std::uint8_t *message = pool.entry(sequence);
            writeTick(message, cfg.size, sequence);
            while (true)
            {
                std::int64_t r = pub->offer(message, static_cast<std::size_t>(cfg.size));
                if (r > 0)
                {
                    return true;
                }
                if (r == PUBLICATION_CLOSED || r == MAX_POSITION_EXCEEDED)
                {
                    return false;
                }
                sub->poll(handler, 1024);
            }
        };

        std::int64_t sequence = 0;
        std::int64_t warmupDeadline = nowNs() + cfg.warmupSeconds * 1000000000LL;
        while (nowNs() < warmupDeadline)
        {
            if (!offerWithDrain(sequence++))
            {
                return;
            }
            sub->poll(handler, 1024);
        }
        while (sub->poll(handler, 1024) > 0)
        {
        }

        received = 0;
        bidSum = 0;
        sequence = 0;
        std::int64_t sent = 0;
        std::int64_t startNs = nowNs();
        std::int64_t runDeadline = startNs + cfg.timeSeconds * 1000000000LL;

        while (nowNs() < runDeadline)
        {
            for (int i = 0; i < 1000; i++)
            {
                if (!offerWithDrain(sequence++))
                {
                    return;
                }
                sent++;
            }
            sub->poll(handler, 1024);
        }
        std::int64_t elapsedNs = nowNs() - startNs;

        while (received < sent)
        {
            if (sub->poll(handler, 1024) == 0)
            {
                break;
            }
        }

        result.sent = sent;
        result.received = received;
        result.elapsedNs = elapsedNs;
        result.bidSum = bidSum;
        result.failed = false;

        pub.reset();
        sub.reset();
    }
    catch (const std::exception &e)
    {
        std::fprintf(stderr, "[bench-cpp-mp] worker %d error: %s\n", result.index, e.what());
    }
}

int main(int argc, char **argv)
{
    Config cfg = parseArgs(argc, argv);

    std::fprintf(stderr, "[bench-cpp-mp] workers=%d size=%d time=%ds warmup=%ds dir=%s\n",
        cfg.workers, cfg.size, cfg.timeSeconds, cfg.warmupSeconds, cfg.aeronDir.c_str());

    std::vector<WorkerResult> results(static_cast<std::size_t>(cfg.workers));
    std::vector<std::thread> threads;
    threads.reserve(static_cast<std::size_t>(cfg.workers));

    for (int i = 0; i < cfg.workers; i++)
    {
        results[static_cast<std::size_t>(i)].index = i;
        threads.emplace_back(workerMain, std::cref(cfg), std::ref(results[static_cast<std::size_t>(i)]));
    }

    std::int64_t totalSent = 0;
    std::int64_t totalReceived = 0;
    std::int64_t maxElapsed = 0;
    std::int64_t bidSum = 0;
    bool anyFailed = false;

    for (int i = 0; i < cfg.workers; i++)
    {
        threads[static_cast<std::size_t>(i)].join();
        const WorkerResult &r = results[static_cast<std::size_t>(i)];
        if (r.failed)
        {
            anyFailed = true;
            continue;
        }
        totalSent += r.sent;
        totalReceived += r.received;
        bidSum += r.bidSum;
        if (r.elapsedNs > maxElapsed)
        {
            maxElapsed = r.elapsedNs;
        }
    }

    if (anyFailed)
    {
        std::fprintf(stderr, "[bench-cpp-mp] a worker failed\n");
        return 1;
    }

    double opsPerSec = maxElapsed == 0 ? 0 : static_cast<double>(totalSent) * 1e9 / static_cast<double>(maxElapsed);

    std::fprintf(stderr, "[bench-cpp-mp] bid_sum=%" PRId64 "\n", bidSum);

    std::printf(
        "{\"client\":\"cpp\",\"scenario\":\"throughput\","
        "\"payload_size\":%d,"
        "\"workers\":%d,"
        "\"samples\":%" PRId64 ","
        "\"received\":%" PRId64 ","
        "\"ops_per_sec\":%.3f,"
        "\"bytes_per_sec\":%.3f,"
        "\"elapsed_ms\":%.3f}\n",
        cfg.size,
        cfg.workers,
        totalSent,
        totalReceived,
        opsPerSec,
        opsPerSec * cfg.size,
        static_cast<double>(maxElapsed) / 1e6);

    return 0;
}
