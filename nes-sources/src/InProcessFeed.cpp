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

#include <Sources/InProcessFeed.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <ErrorHandling.hpp>

namespace NES
{

InProcessFeed::InProcessFeed(const size_t capacity) : queue(capacity)
{
    PRECONDITION(capacity > 0, "An InProcessFeed needs a capacity of at least one row");
}

bool InProcessFeed::tryPush(std::string row)
{
    if (queue.writeIfNotFull(std::move(row)))
    {
        return true;
    }
    dropped.fetch_add(1, std::memory_order_relaxed);
    return false;
}

std::optional<std::string> InProcessFeed::tryPop(const std::chrono::milliseconds timeout)
{
    std::string row;
    if (queue.tryReadUntil(std::chrono::steady_clock::now() + timeout, row))
    {
        return row;
    }
    return std::nullopt;
}

size_t InProcessFeed::droppedRows() const
{
    return dropped.load(std::memory_order_relaxed);
}

size_t InProcessFeed::capacity() const
{
    return queue.capacity();
}

InProcessFeedRegistry& InProcessFeedRegistry::instance()
{
    static InProcessFeedRegistry registry;
    return registry;
}

std::shared_ptr<InProcessFeed> InProcessFeedRegistry::getOrCreate(const std::string& name, const size_t capacity)
{
    const std::lock_guard lock(mutex);
    if (const auto existing = feeds.find(name); existing != feeds.end())
    {
        return existing->second;
    }
    return feeds.emplace(name, std::make_shared<InProcessFeed>(capacity)).first->second;
}

void InProcessFeedRegistry::clear()
{
    const std::lock_guard lock(mutex);
    feeds.clear();
}

}
