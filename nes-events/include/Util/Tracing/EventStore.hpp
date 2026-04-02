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

#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <variant>
#include <Util/UUID.hpp>
#include <folly/MPMCQueue.h>
#include <WorkerLocalSingleton.hpp>

namespace NES
{

static constexpr size_t EVENT_MAX_STR_LEN = 64;

std::pair<uint64_t, uint64_t> uuidToU64Pair(const UUID& uuid);

/// Typed event payloads.
struct PipelineCompiledEvent
{
    uint64_t timestamp_ns;
    uint64_t query_id_hi;
    uint64_t query_id_lo;
    uint64_t pipeline_id;
    uint64_t compile_time_ns;
};

struct QueryStatusEvent
{
    uint64_t timestamp_ns;
    uint64_t query_id_hi;
    uint64_t query_id_lo;
    char status[EVENT_MAX_STR_LEN];
};

struct UnpooledBufferAllocEvent
{
    uint64_t timestamp_ns;
    uint64_t alloc_size;
};

struct BufferIngestionEvent
{
    uint64_t timestamp_ns;
    uint64_t query_id_hi;
    uint64_t query_id_lo;
    uint64_t origin_id;
    uint64_t num_tuples;
};

using LogEventVariant = std::variant<PipelineCompiledEvent, QueryStatusEvent, UnpooledBufferAllocEvent, BufferIngestionEvent>;

enum class EventType : uint8_t
{
    pipeline_compiled,
    query_status,
    unpooled_buffer_alloc,
    buffer_ingestion,
};

/// Map EventType → variant alternative type (must stay in header — used by template emitEvent).
template <EventType>
struct EventPayload;

template <>
struct EventPayload<EventType::pipeline_compiled>
{
    using type = PipelineCompiledEvent;
};

template <>
struct EventPayload<EventType::query_status>
{
    using type = QueryStatusEvent;
};

template <>
struct EventPayload<EventType::unpooled_buffer_alloc>
{
    using type = UnpooledBufferAllocEvent;
};

template <>
struct EventPayload<EventType::buffer_ingestion>
{
    using type = BufferIngestionEvent;
};

class EventSubscriber
{
public:
    explicit EventSubscriber(size_t capacity = 16384);
    bool tryPush(const LogEventVariant& event);
    bool tryPop(LogEventVariant& event);

    template <typename Clock>
    bool tryReadUntil(const std::chrono::time_point<Clock>& deadline, LogEventVariant& event)
    {
        return queue.tryReadUntil(deadline, event);
    }

private:
    folly::MPMCQueue<LogEventVariant> queue;
};

/// Per-thread event store. Inherits from WorkerLocalSingleton so that constructing
/// an EventStore on a worker thread automatically sets it as the thread-local instance
/// and propagates to all child threads created via NES::Thread.
///
/// Lock-free on the emit (hot) path.
class EventStore : public WorkerLocalSingleton<EventStore>
{
public:
    [[nodiscard]] bool hasSubscribers(EventType type) const noexcept;
    void subscribe(EventType type, std::shared_ptr<EventSubscriber> subscriber);
    void unsubscribe(EventType type);
    void emit(EventType type, const LogEventVariant& event);

    static std::optional<EventType> fromName(std::string_view name);
    static std::string_view toName(EventType type);

private:
    static constexpr size_t idx(EventType type) { return static_cast<size_t>(type); }

    std::array<std::atomic<EventSubscriber*>, 4> slots{};
    std::array<std::shared_ptr<EventSubscriber>, 4> owned{};
};

namespace detail
{
uint64_t steadyClockNs();

void emitPipelineCompiled(const UUID& query_id, uint64_t pipeline_id, uint64_t compile_time_ns);
void emitQueryStatus(const UUID& query_id, const char* status);
void emitBufferIngestion(const UUID& query_id, uint64_t origin_id, uint64_t num_tuples);

/// Template — must stay in header.
template <EventType Type, typename... Args>
void emitEvent(Args... args)
{
    auto* store = EventStore::tryInstance();
    if (!store || !store->hasSubscribers(Type))
        return;
    using Payload = typename EventPayload<Type>::type;
    Payload payload{steadyClockNs(), static_cast<decltype(std::declval<Payload>().timestamp_ns)>(args)...};
    store->emit(Type, LogEventVariant(payload));
}

}
}

/// clang-format off

#define NES_EMIT_pipeline_compiled(...) ::NES::detail::emitPipelineCompiled(__VA_ARGS__)
#define NES_EMIT_query_status(...) ::NES::detail::emitQueryStatus(__VA_ARGS__)
#define NES_EMIT_unpooled_buffer_alloc(...) ::NES::detail::emitEvent<::NES::EventType::unpooled_buffer_alloc>(__VA_ARGS__)
#define NES_EMIT_buffer_ingestion(...) ::NES::detail::emitBufferIngestion(__VA_ARGS__)
#define NES_EVENT(type, ...) NES_EMIT_##type(__VA_ARGS__)
#define NES_EVENT0(type) \
    do \
    { \
        auto* store_ = ::NES::EventStore::tryInstance(); \
        if (store_ && store_->hasSubscribers(::NES::EventType::type)) \
        { \
            using Payload_ = typename ::NES::EventPayload<::NES::EventType::type>::type; \
            Payload_ ev_{}; \
            ev_.timestamp_ns = ::NES::detail::steadyClockNs(); \
            store_->emit(::NES::EventType::type, ::NES::LogEventVariant(ev_)); \
        } \
    } while (0)

/// clang-format on
