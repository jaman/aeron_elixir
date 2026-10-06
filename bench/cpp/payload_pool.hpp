#ifndef AERON_BENCH_PAYLOAD_POOL_HPP
#define AERON_BENCH_PAYLOAD_POOL_HPP

#include <cstddef>
#include <cstdint>
#include <new>

#include "../c/payload_pool.h"

class PayloadPool
{
public:
    explicit PayloadPool(std::size_t size)
    {
        if (payload_pool_init(&m_pool, size) < 0)
        {
            throw std::bad_alloc();
        }
    }

    ~PayloadPool()
    {
        payload_pool_close(&m_pool);
    }

    PayloadPool(const PayloadPool &) = delete;
    PayloadPool &operator=(const PayloadPool &) = delete;

    std::uint8_t *entry(std::int64_t sequence) const
    {
        return payload_pool_entry(&m_pool, sequence);
    }

private:
    payload_pool_t m_pool;
};

#endif
