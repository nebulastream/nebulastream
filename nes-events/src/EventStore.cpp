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

#include <Util/Tracing/EventStore.hpp>

#include <array>
#include <bit>
#include <cstring>
#include <magic_enum/magic_enum.hpp>

namespace NES
{

std::pair<uint64_t, uint64_t> uuidToU64Pair(const UUID& uuid)
{
    auto halves = std::bit_cast<std::array<uint64_t, 2>>(uuid);
    return {halves[0], halves[1]};
}

/// --- EventSubscriber ---

EventSubscriber::EventSubscriber(size_t capacity) : queue(capacity)
{
}

bool EventSubscriber::tryPush(const LogEventVariant& event)
{
    return queue.write(event);
}

bool EventSubscriber::tryPop(LogEventVariant& event)
{
    return queue.read(event);
}

/// --- EventStore ---

bool EventStore::hasSubscribers(EventType type) const noexcept
{
    return slots[idx(type)].load(std::memory_order_relaxed) != nullptr;
}

void EventStore::subscribe(EventType type, std::shared_ptr<EventSubscriber> subscriber)
{
    owned[idx(type)] = subscriber;
    slots[idx(type)].store(subscriber.get(), std::memory_order_release);
}

void EventStore::unsubscribe(EventType type)
{
    slots[idx(type)].store(nullptr, std::memory_order_release);
    owned[idx(type)].reset();
}

void EventStore::emit(EventType type, const LogEventVariant& event)
{
    if (auto* sub = slots[idx(type)].load(std::memory_order_relaxed))
    {
        sub->tryPush(event);
    }
}

std::optional<EventType> EventStore::fromName(std::string_view name)
{
    return magic_enum::enum_cast<EventType>(name);
}

std::string_view EventStore::toName(EventType type)
{
    return magic_enum::enum_name(type);
}

/// --- detail emit helpers ---

namespace detail
{

uint64_t steadyClockNs()
{
    return static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
}

void emitPipelineCompiled(const UUID& query_id, uint64_t pipeline_id, uint64_t compile_time_ns)
{
    auto* store = EventStore::tryInstance();
    if (!store || !store->hasSubscribers(EventType::pipeline_compiled))
        return;
    auto [hi, lo] = uuidToU64Pair(query_id);
    PipelineCompiledEvent payload{steadyClockNs(), hi, lo, pipeline_id, compile_time_ns};
    store->emit(EventType::pipeline_compiled, LogEventVariant(payload));
}

void emitQueryStatus(const UUID& query_id, const char* status)
{
    auto* store = EventStore::tryInstance();
    if (!store || !store->hasSubscribers(EventType::query_status))
        return;
    QueryStatusEvent payload{};
    payload.timestamp_ns = steadyClockNs();
    auto [hi, lo] = uuidToU64Pair(query_id);
    payload.query_id_hi = hi;
    payload.query_id_lo = lo;
    if (status)
    {
        std::strncpy(payload.status, status, EVENT_MAX_STR_LEN - 1);
        payload.status[EVENT_MAX_STR_LEN - 1] = '\0';
    }
    store->emit(EventType::query_status, LogEventVariant(payload));
}

void emitBufferIngestion(const UUID& query_id, uint64_t origin_id, uint64_t num_tuples)
{
    auto* store = EventStore::tryInstance();
    if (!store || !store->hasSubscribers(EventType::buffer_ingestion))
        return;
    auto [hi, lo] = uuidToU64Pair(query_id);
    BufferIngestionEvent payload{steadyClockNs(), hi, lo, origin_id, num_tuples};
    store->emit(EventType::buffer_ingestion, LogEventVariant(payload));
}

}
}
