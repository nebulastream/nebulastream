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
    #include <cstddef>
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
    #include <tuple>
    #include <type_traits>
    #include <utility>
    #include <vector>
    #include <unistd.h>
    #include <fmt/format.h>
    #include <rfl/to_view.hpp>

/// File format, read by scripts/latency_trace.py. All integers are little endian.
///
///   header:  "NESLAT03", pid u64, steady clock ns u64, system clock ns u64, hostname char[64]
///   schemas: count u16, then per event type:
///              id u16, name, category u8, payload size u32, field count u16,
///              per field: name, type u8 ('k' Key, 'u' uint64_t, 'i' int64_t), offset u32
///            where a name is a length u16 followed by its bytes
///   chunks:  until the end of the file: thread u16, length u32, then `length` bytes of records of that thread
///   record:  type u16, kind u8 (EventTrace::Kind), time u64, then the event's payload
namespace NES::EventTrace::Ring
{
namespace
{

constexpr size_t RING_BYTES = 1 << 21;
constexpr size_t RECORD_HEADER_BYTES = sizeof(uint16_t) + sizeof(Kind) + sizeof(uint64_t);
constexpr auto FLUSH_INTERVAL = std::chrono::milliseconds(10);
constexpr std::array<char, 8> MAGIC{'N', 'E', 'S', 'L', 'A', 'T', '0', '3'};

/// Single producer (the owning thread), single consumer (the flusher). Positions count bytes and only grow; a record that reaches
/// the end of the array continues at its start.
struct RingBuffer
{
    explicit RingBuffer(uint16_t thread) : thread(thread) { }

    uint16_t thread;
    std::array<std::byte, RING_BYTES> bytes{};
    std::atomic<size_t> head{0};
    std::atomic<size_t> tail{0};
    std::atomic<bool> alive{true};

    void put(size_t position, const void* source, size_t size) noexcept
    {
        const auto index = position % RING_BYTES;
        const auto first = std::min(size, RING_BYTES - index);
        std::memcpy(&bytes[index], source, first);
        std::memcpy(bytes.data(), static_cast<const std::byte*>(source) + first, size - first);
    }
};

template <typename T>
void append(std::string& out, T value)
{
    out.append(reinterpret_cast<const char*>(&value), sizeof(value)); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
}

void appendName(std::string& out, std::string_view name)
{
    append(out, static_cast<uint16_t>(name.size()));
    out.append(name);
}

template <typename Field>
constexpr char fieldType()
{
    if constexpr (std::is_same_v<Field, Key>)
    {
        return 'k';
    }
    else if constexpr (std::is_same_v<Field, uint64_t>)
    {
        return 'u';
    }
    else
    {
        static_assert(std::is_same_v<Field, int64_t>, "Event fields are Key, uint64_t or int64_t");
        return 'i';
    }
}

template <typename Event>
void appendSchema(std::string& out)
{
    append(out, typeId<Event>);
    appendName(out, Event::name);
    append(out, Event::category);
    append(out, payloadSize<Event>);
    std::string fields;
    uint16_t count = 0;
    if constexpr (!std::is_empty_v<Event>)
    {
        Event instance{};
        rfl::to_view(instance).apply(
            [&](const auto& field)
            {
                const auto* address
                    = reinterpret_cast<const std::byte*>(field.value()); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                appendName(fields, field.name());
                append(fields, fieldType<std::remove_cvref_t<decltype(*field.value())>>());
                append(
                    fields,
                    static_cast<uint32_t>(
                        address - reinterpret_cast<const std::byte*>(&instance))); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                ++count;
            });
    }
    append(out, count);
    out.append(fields);
}

std::string schemas()
{
    std::string out;
    append(out, static_cast<uint16_t>(std::tuple_size_v<EventTypes>));
    [&]<typename... Events>(std::tuple<Events...>*) { (appendSchema<Events>(out), ...); }(static_cast<EventTypes*>(nullptr));
    return out;
}

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
            const auto schema = schemas();
            std::fwrite(schema.data(), 1, schema.size(), file);
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
            if (head == tail || file == nullptr)
            {
                ring->tail.store(head, std::memory_order_release);
                continue;
            }
            /// The producer publishes whole records, so a chunk always ends at a record boundary.
            const auto length = static_cast<uint32_t>(head - tail);
            std::fwrite(&ring->thread, sizeof(ring->thread), 1, file);
            std::fwrite(&length, sizeof(length), 1, file);
            const auto index = tail % RING_BYTES;
            const auto first = std::min<size_t>(length, RING_BYTES - index);
            std::fwrite(&ring->bytes[index], 1, first, file);
            std::fwrite(ring->bytes.data(), 1, length - first, file);
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
}

void record(uint16_t type, Kind kind, const void* payload, uint32_t size) noexcept
{
    thread_local LocalRing local;
    auto& ring = *local.ring;
    const auto head = ring.head.load(std::memory_order_relaxed);
    const auto total = RECORD_HEADER_BYTES + size;
    if (RING_BYTES - (head - ring.tail.load(std::memory_order_acquire)) < total)
    {
        recorder().dropped.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    std::array<std::byte, RECORD_HEADER_BYTES> header; /// NOLINT(cppcoreguidelines-pro-type-member-init) fully written below
    const auto time = now();
    std::memcpy(header.data(), &type, sizeof(type));
    std::memcpy(header.data() + sizeof(type), &kind, sizeof(kind));
    std::memcpy(header.data() + sizeof(type) + sizeof(kind), &time, sizeof(time));
    ring.put(head, header.data(), header.size());
    ring.put(head + header.size(), payload, size);
    ring.head.store(head + total, std::memory_order_release);
}

void writePlan(std::string_view plan)
{
    recorder().writePlan(plan);
}

void counterCreated(uint64_t id, std::string_view name)
{
    recorder().writePlan(fmt::format("counter {} {}\n", id, name));
}

}

#endif
