/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

/// A baseline for what two processes can exchange through queues in shared memory, to compare the UDF processes against. It is
/// independent of NebulaStream:
///
///     clang++ -O3 -std=c++20 -o shm_queue_baseline shm_queue_baseline.cpp
///     ./shm_queue_baseline <producer cpu> <consumer cpu> [seconds per measurement]
///
/// A producer and a consumer process share two single-producer single-consumer rings of 4 MiB, one per direction, as a worker and a
/// UDF process do. Each measurement prints messages and payload bytes per second:
///   oneway  the producer sends messages, the consumer takes them
///   echo    the consumer answers every message with one of the same size, with up to `window` messages in flight (1 is ping-pong)
/// with the payload either copied through the ring (copy) or left in a shared pool, so that the ring only carries where it is
/// (inplace), and with the consumer either ignoring the payload (skip) or reading all of it (read). The sides wait by spinning, or
/// by sleeping on a futex when a ring stays empty (futex).

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <utility>
#include <vector>
#include <immintrin.h>
#include <sched.h>
#include <unistd.h>
#include <linux/futex.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <sys/wait.h>

namespace
{
constexpr uint64_t RING_SIZE = uint64_t{4} * 1024 * 1024;
constexpr uint64_t POOL_SIZE = uint64_t{256} * 1024 * 1024;
constexpr uint64_t WRAP = ~uint64_t{0};
constexpr uint32_t STOP = ~uint32_t{0};

/// The words of a ring, each on a cache line of its own
struct Ring
{
    alignas(64) std::atomic<uint64_t> tail;
    alignas(64) std::atomic<uint64_t> head;
    /// Counts publications, for the futex of a consumer that sleeps
    alignas(64) std::atomic<uint32_t> published;
    alignas(64) std::atomic<uint32_t> consumerSleeps;
    alignas(64) std::byte data[RING_SIZE];
};

struct Shared
{
    Ring requests;
    Ring answers;
    alignas(64) std::atomic<uint64_t> consumed;
    alignas(64) std::atomic<uint64_t> checksum;
    alignas(64) std::byte pool[POOL_SIZE];
};

enum class Wait : uint8_t
{
    SPIN,
    FUTEX,
};

void futexWait(std::atomic<uint32_t>& word, const uint32_t expected)
{
    syscall(SYS_futex, &word, FUTEX_WAIT, expected, nullptr, nullptr, 0);
}

void futexWake(std::atomic<uint32_t>& word)
{
    syscall(SYS_futex, &word, FUTEX_WAKE, 1, nullptr, nullptr, 0);
}

/// The producer side of a ring: records are [size][payload, padded to 8 bytes], and a size of WRAP sends the reader to the start.
struct Writer
{
    Ring& ring;
    Wait wait;
    uint64_t tail = 0;
    uint64_t cachedHead = 0;

    std::byte* reserve(const uint64_t payload)
    {
        const uint64_t record = 8 + ((payload + 7) & ~uint64_t{7});
        auto position = tail % RING_SIZE;
        const uint64_t skip = RING_SIZE - position < record ? RING_SIZE - position : 0;
        while (RING_SIZE - (tail - cachedHead) < skip + record)
        {
            cachedHead = ring.head.load(std::memory_order_acquire);
            if (RING_SIZE - (tail - cachedHead) < skip + record)
            {
                _mm_pause();
            }
        }
        if (skip != 0)
        {
            std::memcpy(ring.data + position, &WRAP, 8);
            tail += skip;
            position = 0;
        }
        std::memcpy(ring.data + position, &payload, 8);
        pending = record;
        return ring.data + position + 8;
    }

    void publish()
    {
        tail += pending;
        ring.tail.store(tail, std::memory_order_release);
        if (wait == Wait::FUTEX)
        {
            ring.published.fetch_add(1, std::memory_order_seq_cst);
            if (ring.consumerSleeps.load(std::memory_order_seq_cst) != 0)
            {
                futexWake(ring.published);
            }
        }
    }

    uint64_t pending = 0;
};

struct Reader
{
    Ring& ring;
    Wait wait;
    uint64_t head = 0;
    uint64_t cachedTail = 0;

    /// The next record, blocking until there is one
    std::pair<const std::byte*, uint64_t> next()
    {
        while (true)
        {
            if (head == cachedTail)
            {
                cachedTail = ring.tail.load(std::memory_order_acquire);
                uint32_t spins = 0;
                while (head == cachedTail)
                {
                    if (wait == Wait::FUTEX && ++spins > 2000)
                    {
                        const auto seen = ring.published.load(std::memory_order_seq_cst);
                        ring.consumerSleeps.store(1, std::memory_order_seq_cst);
                        if (ring.tail.load(std::memory_order_seq_cst) == head)
                        {
                            futexWait(ring.published, seen);
                        }
                        ring.consumerSleeps.store(0, std::memory_order_seq_cst);
                        spins = 0;
                    }
                    else
                    {
                        _mm_pause();
                    }
                    cachedTail = ring.tail.load(std::memory_order_acquire);
                }
            }
            const auto position = head % RING_SIZE;
            uint64_t payload = 0;
            std::memcpy(&payload, ring.data + position, 8);
            if (payload == WRAP)
            {
                head += RING_SIZE - position;
                continue;
            }
            pending = 8 + ((payload + 7) & ~uint64_t{7});
            return {ring.data + position + 8, payload};
        }
    }

    void release()
    {
        head += pending;
        ring.head.store(head, std::memory_order_release);
    }

    [[nodiscard]] bool hasNext()
    {
        if (head != cachedTail)
        {
            return true;
        }
        cachedTail = ring.tail.load(std::memory_order_acquire);
        return head != cachedTail;
    }

    uint64_t pending = 0;
};

/// Where a message is in the pool, which is all an in-place message carries
struct Location
{
    uint64_t offset;
    uint32_t size;
    uint32_t stop;
};

uint64_t sum(const std::byte* data, const uint64_t size)
{
    uint64_t total = 0;
    for (uint64_t i = 0; i + 8 <= size; i += 8)
    {
        uint64_t word = 0;
        std::memcpy(&word, data + i, 8);
        total += word;
    }
    return total;
}

void pin(const int cpu)
{
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    if (sched_setaffinity(0, sizeof(set), &set) != 0)
    {
        std::perror("sched_setaffinity");
        std::exit(1);
    }
}

struct Config
{
    bool echo;
    bool inPlace;
    bool read;
    Wait wait;
    uint32_t size;
    uint32_t window;
};

/// Takes messages until the producer says stop. With echo, answers each with a message of the same size.
void consume(Shared& shared, const Config config)
{
    Reader requests{shared.requests, config.wait};
    Writer answers{shared.answers, config.wait};
    uint64_t consumed = 0;
    uint64_t checksum = 0;
    while (true)
    {
        const auto [data, size] = requests.next();
        const std::byte* payload = data;
        uint64_t payloadSize = size;
        Location location{};
        if (config.inPlace)
        {
            std::memcpy(&location, data, sizeof(location));
            payload = shared.pool + location.offset;
            payloadSize = location.size;
        }
        if ((config.inPlace && location.stop == STOP) || (!config.inPlace && size == 0))
        {
            requests.release();
            break;
        }
        if (config.read)
        {
            checksum += sum(payload, payloadSize);
        }
        if (config.echo)
        {
            if (config.inPlace)
            {
                /// The answer is written into the message, as a UDF process writes its results into the batch
                if (config.read)
                {
                    std::memcpy(shared.pool + location.offset, &checksum, 8);
                }
                auto* answer = answers.reserve(sizeof(location));
                std::memcpy(answer, &location, sizeof(location));
            }
            else
            {
                auto* answer = answers.reserve(payloadSize);
                std::memcpy(answer, payload, payloadSize);
            }
            answers.publish();
        }
        requests.release();
        ++consumed;
        if ((consumed & 1023) == 0)
        {
            shared.consumed.store(consumed, std::memory_order_relaxed);
        }
    }
    shared.consumed.store(consumed, std::memory_order_release);
    shared.checksum.store(checksum, std::memory_order_release);
}

void measure(Shared& shared, const Config config, const int producerCpu, const int consumerCpu, const double seconds)
{
    shared.requests.tail = 0;
    shared.requests.head = 0;
    shared.answers.tail = 0;
    shared.answers.head = 0;
    shared.consumed = 0;

    const auto child = fork();
    if (child == 0)
    {
        pin(consumerCpu);
        consume(shared, config);
        _exit(0);
    }
    pin(producerCpu);
    Writer requests{shared.requests, config.wait};
    Reader answers{shared.answers, config.wait};
    std::vector<std::byte> message(config.size, std::byte{1});
    /// In-place messages cycle through the pool, as batches cycle through a buffer pool
    const uint64_t slots = POOL_SIZE / std::max<uint64_t>(config.size, 64);
    uint64_t slot = 0;
    uint64_t sent = 0;
    uint64_t answered = 0;
    uint64_t checksum = 0;

    const auto takeAnswer = [&]
    {
        const auto [data, size] = answers.next();
        if (config.read)
        {
            checksum += config.inPlace ? sum(shared.pool + reinterpret_cast<const Location*>(data)->offset, 8) : sum(data, size);
        }
        answers.release();
        ++answered;
    };

    const auto start = std::chrono::steady_clock::now();
    const auto deadline = start + std::chrono::duration<double>(seconds);
    while (true)
    {
        if ((sent & 255) == 0 && std::chrono::steady_clock::now() >= deadline)
        {
            break;
        }
        if (config.echo)
        {
            while (sent - answered >= config.window)
            {
                takeAnswer();
            }
        }
        if (config.inPlace)
        {
            const Location location{.offset = (slot++ % slots) * std::max<uint64_t>(config.size, 64), .size = config.size, .stop = 0};
            /// The producer fills the message, as a worker thread fills a batch
            std::memcpy(shared.pool + location.offset, message.data(), config.size);
            std::memcpy(requests.reserve(sizeof(location)), &location, sizeof(location));
        }
        else
        {
            std::memcpy(requests.reserve(config.size), message.data(), config.size);
        }
        requests.publish();
        ++sent;
    }
    while (config.echo && answered < sent)
    {
        takeAnswer();
    }
    if (config.inPlace)
    {
        const Location stop{.offset = 0, .size = 0, .stop = STOP};
        std::memcpy(requests.reserve(sizeof(stop)), &stop, sizeof(stop));
    }
    else
    {
        requests.reserve(0);
    }
    requests.publish();
    waitpid(child, nullptr, 0);
    const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    const auto messages = static_cast<double>(shared.consumed.load()) / elapsed;
    std::printf(
        "%-6s %-7s %-4s %-5s size %8u window %4u  %12.0f msg/s  %8.2f GB/s  %8.1f ns/msg\n",
        config.echo ? "echo" : "oneway",
        config.inPlace ? "inplace" : "copy",
        config.read ? "read" : "skip",
        config.wait == Wait::SPIN ? "spin" : "futex",
        config.size,
        config.echo ? config.window : 0,
        messages,
        messages * config.size / 1e9,
        1e9 / messages);
    std::fflush(stdout);
}
}

int main(const int argc, char** argv)
{
    if (argc < 3)
    {
        std::fprintf(stderr, "usage: %s <producer cpu> <consumer cpu> [seconds per measurement]\n", argv[0]);
        return 1;
    }
    const int producerCpu = std::atoi(argv[1]);
    const int consumerCpu = std::atoi(argv[2]);
    const double seconds = argc > 3 ? std::atof(argv[3]) : 1.0;

    auto* shared = static_cast<Shared*>(mmap(nullptr, sizeof(Shared), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0));
    if (shared == MAP_FAILED)
    {
        std::perror("mmap");
        return 1;
    }
    pin(producerCpu);
    /// Touch everything once, so that page faults are not measured
    std::memset(static_cast<void*>(shared), 0, sizeof(Shared));

    for (const auto wait : {Wait::SPIN, Wait::FUTEX})
    {
        for (const uint32_t size : {64U, 4096U, 65536U, 1048576U})
        {
            for (const bool inPlace : {false, true})
            {
                for (const bool read : {false, true})
                {
                    measure(
                        *shared,
                        {.echo = false, .inPlace = inPlace, .read = read, .wait = wait, .size = size, .window = 0},
                        producerCpu,
                        consumerCpu,
                        seconds);
                }
                for (const uint32_t window : {1U, 8U})
                {
                    /// Larger messages than the ring holds a window of cannot be in flight at once when they are copied
                    if (!inPlace && uint64_t{window} * size > RING_SIZE / 2)
                    {
                        continue;
                    }
                    measure(
                        *shared,
                        {.echo = true, .inPlace = inPlace, .read = true, .wait = wait, .size = size, .window = window},
                        producerCpu,
                        consumerCpu,
                        seconds);
                }
            }
        }
    }
    return 0;
}
