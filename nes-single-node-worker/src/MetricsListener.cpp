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

#include <MetricsListener.hpp>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <thread>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/SystemEventListener.hpp>
#include <QueryEngineStatisticListener.hpp>
#include <QueryId.hpp>
#include <QueryStatus.hpp>

namespace NES
{

MetricsListener::MetricsListener() : shards(std::max(std::thread::hardware_concurrency(), 1U))
{
}

MetricsListener::Shard& MetricsListener::shardFor(const WorkerThreadId threadId)
{
    return shards[threadId.getRawValue() % shards.size()];
}

MetricsListener::AtomicCounters& MetricsListener::countersFor(Shard& shard, const QueryId& queryId)
{
    {
        const auto locked = shard.counters.rlock();
        if (const auto it = locked->find(queryId); it != locked->end())
        {
            /// Entries are never erased, so the reference stays valid after the lock is released.
            return const_cast<AtomicCounters&>(it->second);
        }
    }
    return shard.counters.wlock()->try_emplace(queryId).first->second;
}

void MetricsListener::onEvent(Event event)
{
    std::visit(
        [this]<typename E>(const E& typedEvent)
        {
            if constexpr (std::is_same_v<E, TaskExecutionStart>)
            {
                countersFor(shardFor(typedEvent.threadId), typedEvent.queryId)
                    .processedTuples.fetch_add(typedEvent.numberOfTuples, std::memory_order_relaxed);
            }
            else if constexpr (std::is_same_v<E, TaskExecutionComplete>)
            {
                countersFor(shardFor(typedEvent.threadId), typedEvent.queryId).processedTasks.fetch_add(1, std::memory_order_relaxed);
            }
            else if constexpr (std::is_same_v<E, TaskExpired>)
            {
                countersFor(shardFor(typedEvent.threadId), typedEvent.queryId).expiredTasks.fetch_add(1, std::memory_order_relaxed);
            }
        },
        event);
}

void MetricsListener::onEvent(SystemEvent)
{
}

QueryCounters MetricsListener::getCounters(const QueryId& queryId) const
{
    QueryCounters total{};
    for (const auto& shard : shards)
    {
        const auto locked = shard.counters.rlock();
        if (const auto it = locked->find(queryId); it != locked->end())
        {
            total.processedTuples += it->second.processedTuples.load(std::memory_order_relaxed);
            total.processedTasks += it->second.processedTasks.load(std::memory_order_relaxed);
            total.expiredTasks += it->second.expiredTasks.load(std::memory_order_relaxed);
        }
    }
    return total;
}

}
