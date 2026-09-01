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

#include <Feeds/EventFeed.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

EventFeed::EventFeed(const size_t capacity) : queue(capacity)
{
    PRECONDITION(capacity > 0, "An EventFeed needs a capacity of at least one row");
}

bool EventFeed::tryPush(std::string row)
{
    /// 'write' rather than 'writeIfNotFull': both reject a full queue, but 'writeIfNotFull' can additionally
    /// wait for a read that has been assigned a ticket to complete. Waiting is what a producer on the query
    /// engine's worker threads must never do, and this path is allowed to drop.
    if (queue.write(std::move(row)))
    {
        return true;
    }
    dropped.fetch_add(1, std::memory_order_relaxed);
    return false;
}

std::optional<std::string> EventFeed::tryPop(const std::chrono::milliseconds timeout)
{
    std::string row;
    if (queue.tryReadUntil(std::chrono::steady_clock::now() + timeout, row))
    {
        return row;
    }
    return std::nullopt;
}

bool EventFeed::hasConsumer() const
{
    return consumerAttached.load(std::memory_order_relaxed);
}

size_t EventFeed::droppedRows() const
{
    return dropped.load(std::memory_order_relaxed);
}

size_t EventFeed::capacity() const
{
    return queue.capacity();
}

bool EventFeed::tryAttachConsumer()
{
    bool expected = false;
    return consumerAttached.compare_exchange_strong(expected, true, std::memory_order_acq_rel);
}

void EventFeed::detachConsumer()
{
    consumerAttached.store(false, std::memory_order_release);
}

EventFeedConsumer::EventFeedConsumer(std::shared_ptr<EventFeed> feed) : feed(std::move(feed))
{
}

EventFeedConsumer::~EventFeedConsumer()
{
    if (feed)
    {
        feed->detachConsumer();
    }
}

EventFeedConsumer::EventFeedConsumer(EventFeedConsumer&& other) noexcept : feed(std::exchange(other.feed, nullptr))
{
}

EventFeedConsumer& EventFeedConsumer::operator=(EventFeedConsumer&& other) noexcept
{
    if (this != &other)
    {
        if (feed)
        {
            feed->detachConsumer();
        }
        feed = std::exchange(other.feed, nullptr);
    }
    return *this;
}

EventFeed& EventFeedConsumer::operator*() const
{
    INVARIANT(feed != nullptr, "Dereferenced an EventFeedConsumer that holds no feed");
    return *feed;
}

EventFeed* EventFeedConsumer::operator->() const
{
    INVARIANT(feed != nullptr, "Dereferenced an EventFeedConsumer that holds no feed");
    return feed.get();
}

EventFeedProducer::EventFeedProducer(std::shared_ptr<EventFeed> feed, std::string host, std::string name)
    : feed(std::move(feed)), host(std::move(host)), name(std::move(name))
{
}

EventFeedProducer::~EventFeedProducer()
{
    if (feed)
    {
        EventFeedRegistry::instance().unregister(host, name);
    }
}

EventFeedProducer::EventFeedProducer(EventFeedProducer&& other) noexcept
    : feed(std::exchange(other.feed, nullptr)), host(std::move(other.host)), name(std::move(other.name))
{
}

EventFeedProducer& EventFeedProducer::operator=(EventFeedProducer&& other) noexcept
{
    if (this != &other)
    {
        if (feed)
        {
            EventFeedRegistry::instance().unregister(host, name);
        }
        feed = std::exchange(other.feed, nullptr);
        host = std::move(other.host);
        name = std::move(other.name);
    }
    return *this;
}

EventFeed& EventFeedProducer::operator*() const
{
    INVARIANT(feed != nullptr, "Dereferenced an EventFeedProducer that holds no feed");
    return *feed;
}

EventFeed* EventFeedProducer::operator->() const
{
    INVARIANT(feed != nullptr, "Dereferenced an EventFeedProducer that holds no feed");
    return feed.get();
}

EventFeedRegistry& EventFeedRegistry::instance()
{
    static EventFeedRegistry registry;
    return registry;
}

EventFeedProducer EventFeedRegistry::create(const Host& host, const std::string_view name, const size_t capacity)
{
    auto key = std::pair{host.getRawValue(), std::string{name}};
    const std::lock_guard lock(mutex);
    if (feeds.contains(key))
    {
        throw CannotOpenSource("Worker {} already publishes a feed named '{}'", host.getRawValue(), name);
    }
    auto feed = std::make_shared<EventFeed>(capacity);
    feeds.emplace(std::move(key), feed);
    return EventFeedProducer{std::move(feed), host.getRawValue(), std::string{name}};
}

EventFeedConsumer EventFeedRegistry::acquire(const Host& host, const std::string_view name)
{
    std::shared_ptr<EventFeed> feed;
    {
        const std::lock_guard lock(mutex);
        if (const auto exact = feeds.find(std::pair{host.getRawValue(), std::string{name}}); exact != feeds.end())
        {
            feed = exact->second;
        }
        else
        {
            /// A worker does not always know itself under the host a query is placed on: the standalone
            /// starter identifies a worker by its data address, which is empty unless one is configured,
            /// while the embedded backend uses the gRPC endpoint. Falling back to an unambiguous match
            /// keeps that case working, and an ambiguous one still fails rather than guessing, which is
            /// what keeps the workers of one process apart.
            for (const auto& [key, candidate] : feeds)
            {
                if (key.second == name)
                {
                    if (feed != nullptr)
                    {
                        throw CannotOpenSource(
                            "Several workers of this process publish a feed named '{}', and none of them is {}", name, host.getRawValue());
                    }
                    feed = candidate;
                }
            }
        }

        if (feed == nullptr)
        {
            throw CannotOpenSource(
                "Worker {} publishes no feed named '{}'. The producing component is most likely not enabled.", host.getRawValue(), name);
        }
    }

    if (!feed->tryAttachConsumer())
    {
        throw CannotOpenSource("The feed '{}' of worker {} is already read by another query", name, host.getRawValue());
    }
    return EventFeedConsumer{std::move(feed)};
}

void EventFeedRegistry::unregister(const std::string& host, const std::string& name)
{
    const std::lock_guard lock(mutex);
    feeds.erase(std::pair{host, name});
}

void EventFeedRegistry::clear()
{
    const std::lock_guard lock(mutex);
    feeds.clear();
}

}
