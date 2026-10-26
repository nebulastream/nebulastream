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

#ifdef NES_EVENT_TRACE_RING

    #include <EventTraceBackends.hpp>

    #include <algorithm>
    #include <array>
    #include <atomic>
    #include <chrono>
    #include <cstdint>
    #include <cstdio>
    #include <cstdlib>
    #include <cstring>
    #include <memory>
    #include <mutex>
    #include <stop_token>
    #include <string>
    #include <string_view>
    #include <thread>
    #include <vector>
    #include <unistd.h>
    #include <fmt/format.h>

namespace NES::EventTrace::Ring
{
namespace
{

constexpr size_t RING_CAPACITY = 1 << 15;
constexpr auto FLUSH_INTERVAL = std::chrono::milliseconds(10);
constexpr std::array<char, 8> MAGIC{'N', 'E', 'S', 'L', 'A', 'T', '0', '2'};

enum class Kind : uint8_t
{
    Point = 0,
    Begin = 1,
    End = 2,
};

/// The layout is the file format read by scripts/latency_trace.py.
struct Event
{
    uint64_t time = 0;
    uint64_t reserved = 0;
    uint64_t origin = 0;
    uint64_t seq = 0;
    uint64_t origin2 = 0;
    /// The `extra` of an end, or the input sequence number of a link.
    uint64_t seq2 = 0;
    uint32_t chunk = 0;
    uint32_t chunk2 = 0;
    uint32_t pipeline = 0;
    uint8_t stage = 0;
    Kind kind = Kind::Point;
    /// Identifies the recording thread within the process, so the stitcher can pair begins and ends.
    uint16_t thread = 0;
};

static_assert(sizeof(Event) == 64, "Event layout is part of the file format");

/// Single producer (the owning thread), single consumer (the flusher).
struct RingBuffer
{
    explicit RingBuffer(uint16_t thread) : thread(thread) { }

    uint16_t thread;
    std::array<Event, RING_CAPACITY> events{};
    std::atomic<size_t> head{0};
    std::atomic<size_t> tail{0};
    std::atomic<bool> alive{true};
};

/// The steady/system clock pair lets the stitcher align events of different processes on the system clock.
struct FileHeader
{
    std::array<char, 8> magic = MAGIC;
    uint64_t pid = 0;
    uint64_t steadyNs = 0;
    uint64_t systemNs = 0;
    std::array<char, 64> hostname{};
};

static_assert(sizeof(FileHeader) == 96, "FileHeader layout is part of the file format");

std::string prefix()
{
    const auto* env = std::getenv("NES_LATENCY_TRACE_PREFIX"); /// NOLINT(concurrency-mt-unsafe) read once during initialization
    return env != nullptr ? std::string(env) : std::string("nes-latency-trace");
}

class Recorder
{
public:
    Recorder()
    {
        const auto base = prefix() + "." + std::to_string(getpid());
        file = std::fopen((base + ".bin").c_str(), "wb"); /// NOLINT(cppcoreguidelines-owning-memory)
        planFile = std::fopen((base + ".plan").c_str(), "w"); /// NOLINT(cppcoreguidelines-owning-memory)
        if (file != nullptr)
        {
            FileHeader header;
            header.pid = static_cast<uint64_t>(getpid());
            header.steadyNs = now();
            header.systemNs
                = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
            gethostname(header.hostname.data(), header.hostname.size() - 1);
            std::fwrite(&header, sizeof(header), 1, file);
        }
        flusher = std::jthread(
            [this](const std::stop_token& stopToken)
            {
                while (!stopToken.stop_requested())
                {
                    std::this_thread::sleep_for(FLUSH_INTERVAL);
                    drain();
                }
            });
    }

    ~Recorder()
    {
        flusher.request_stop();
        flusher.join();
        drain();
        if (const auto droppedEvents = dropped.load(); droppedEvents > 0)
        {
            std::fprintf(
                stderr, "EventTrace: dropped %llu events because a ring buffer was full\n", static_cast<unsigned long long>(droppedEvents));
        }
        if (file != nullptr)
        {
            std::fclose(file); /// NOLINT(cppcoreguidelines-owning-memory)
        }
        if (planFile != nullptr)
        {
            std::fclose(planFile); /// NOLINT(cppcoreguidelines-owning-memory)
        }
    }

    Recorder(const Recorder&) = delete;
    Recorder(Recorder&&) = delete;
    Recorder& operator=(const Recorder&) = delete;
    Recorder& operator=(Recorder&&) = delete;

    std::shared_ptr<RingBuffer> registerRing()
    {
        const std::scoped_lock lock(mutex);
        auto ring = std::make_shared<RingBuffer>(nextThread++);
        rings.push_back(ring);
        return ring;
    }

    void writePlan(std::string_view plan)
    {
        const std::scoped_lock lock(mutex);
        if (planFile != nullptr)
        {
            std::fwrite(plan.data(), 1, plan.size(), planFile);
            std::fflush(planFile);
        }
    }

    std::atomic<uint64_t> dropped{0};

private:
    void drain()
    {
        const std::scoped_lock lock(mutex);
        for (const auto& ring : rings)
        {
            const auto tail = ring->tail.load(std::memory_order_relaxed);
            const auto head = ring->head.load(std::memory_order_acquire);
            for (auto position = tail; position < head;)
            {
                const auto index = position % RING_CAPACITY;
                const auto count = std::min(head - position, RING_CAPACITY - index);
                if (file != nullptr)
                {
                    std::fwrite(&ring->events[index], sizeof(Event), count, file);
                }
                position += count;
            }
            ring->tail.store(head, std::memory_order_release);
        }
        /// A ring of an exited thread can only be removed after its last events were drained above.
        std::erase_if(rings, [](const auto& ring) { return !ring->alive.load(); });
        if (file != nullptr)
        {
            std::fflush(file);
        }
    }

    std::mutex mutex;
    std::vector<std::shared_ptr<RingBuffer>> rings;
    uint16_t nextThread = 0;
    std::FILE* file = nullptr;
    std::FILE* planFile = nullptr;
    std::jthread flusher;
};

Recorder& recorder()
{
    static Recorder instance;
    return instance;
}

struct LocalRing
{
    std::shared_ptr<RingBuffer> ring = recorder().registerRing();
    LocalRing() = default;
    LocalRing(const LocalRing&) = delete;
    LocalRing(LocalRing&&) = delete;
    LocalRing& operator=(const LocalRing&) = delete;
    LocalRing& operator=(LocalRing&&) = delete;

    ~LocalRing() { ring->alive.store(false); }
};

void record(Event event) noexcept
{
    thread_local LocalRing local;
    auto& ring = *local.ring;
    event.thread = ring.thread;
    const auto head = ring.head.load(std::memory_order_relaxed);
    if (head - ring.tail.load(std::memory_order_acquire) == RING_CAPACITY)
    {
        recorder().dropped.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    ring.events[head % RING_CAPACITY] = event;
    ring.head.store(head + 1, std::memory_order_release);
}
}

void begin(Stage stage, const Key& key, uint64_t pipeline) noexcept
{
    record(
        {.time = now(),
         .origin = key.origin,
         .seq = key.seq,
         .chunk = key.chunk,
         .pipeline = static_cast<uint32_t>(pipeline),
         .stage = static_cast<uint8_t>(stage),
         .kind = Kind::Begin});
}

void end(Stage stage, const Key& key, uint64_t pipeline, uint64_t extra) noexcept
{
    record(
        {.time = now(),
         .origin = key.origin,
         .seq = key.seq,
         .seq2 = extra,
         .chunk = key.chunk,
         .pipeline = static_cast<uint32_t>(pipeline),
         .stage = static_cast<uint8_t>(stage),
         .kind = Kind::End});
}

void mark(Stage stage, const Key& key, uint64_t pipeline, uint64_t t) noexcept
{
    record(
        {.time = t,
         .origin = key.origin,
         .seq = key.seq,
         .chunk = key.chunk,
         .pipeline = static_cast<uint32_t>(pipeline),
         .stage = static_cast<uint8_t>(stage)});
}

void link(const Key& output, const Key& input, uint64_t pipeline, uint64_t t) noexcept
{
    record(
        {.time = t,
         .origin = output.origin,
         .seq = output.seq,
         .origin2 = input.origin,
         .seq2 = input.seq,
         .chunk = output.chunk,
         .chunk2 = input.chunk,
         .pipeline = static_cast<uint32_t>(pipeline),
         .stage = static_cast<uint8_t>(Stage::Emit)});
}

void writePlan(std::string_view plan)
{
    recorder().writePlan(plan);
}

void counterCreated(uint32_t id, std::string_view name)
{
    recorder().writePlan(fmt::format("counter {} {}\n", id, name));
}

void counterValue(uint32_t id, int64_t value) noexcept
{
    record({.time = now(), .seq2 = static_cast<uint64_t>(value), .pipeline = id, .stage = static_cast<uint8_t>(Stage::Counter)});
}

}

#endif
