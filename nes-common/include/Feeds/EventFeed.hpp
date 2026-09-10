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

#include <atomic>
#include <chrono>
#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <folly/MPMCQueue.h>

namespace NES
{

/// Names of the feeds a worker publishes. Producer and consumer share these constants, so that no
/// user facing configuration has to name a feed and no query can misspell one.
namespace FeedName
{
constexpr std::string_view ENGINE_EVENTS = "engine_events";
constexpr std::string_view BUFFER_EVENTS = "buffer_events";
}

/// A bounded channel of text rows between a producer somewhere in the worker process and a source that is
/// created later, when a query that reads the feed starts.
///
/// Rows are opaque to the feed. A producer emits whatever the consumer's InputFormatter parses, i.e., one
/// formatted tuple per row without the tuple delimiter, which the source appends.
///
/// Producers must never block, because they may run on latency critical threads such as the query engine's
/// worker threads. 'tryPush' therefore drops the row when the feed is full and counts the drop instead.
///
/// A feed carries a single queue, so it serves exactly one consumer at a time, see 'EventFeedRegistry'.
class EventFeed
{
public:
    explicit EventFeed(size_t capacity);

    /// Non-blocking. Returns false if the row was dropped because the feed is full.
    bool tryPush(std::string row);

    /// Waits at most 'timeout' for a row to become available.
    std::optional<std::string> tryPop(std::chrono::milliseconds timeout);

    /// False while no query reads this feed. Producers check this to skip producing rows nobody reads.
    [[nodiscard]] bool hasConsumer() const;

    [[nodiscard]] size_t droppedRows() const;
    [[nodiscard]] size_t capacity() const;

private:
    friend class EventFeedConsumer;
    friend class EventFeedRegistry;

    /// Returns false if a consumer is already attached.
    bool tryAttachConsumer();
    void detachConsumer();

    folly::MPMCQueue<std::string> queue;
    std::atomic<size_t> dropped{0};
    std::atomic<bool> consumerAttached{false};
};

/// Exclusive read access to a feed, released when the handle dies.
///
/// A feed hands each row to exactly one reader, so two concurrent consumers would divide the rows between
/// them and each observe a different half of the stream instead of the stream. Multiplexing one stream to
/// several readers is a separate concern and belongs to an abstraction that does only that.
class EventFeedConsumer
{
public:
    EventFeedConsumer() = default;
    ~EventFeedConsumer();

    EventFeedConsumer(EventFeedConsumer&& other) noexcept;
    EventFeedConsumer& operator=(EventFeedConsumer&& other) noexcept;
    EventFeedConsumer(const EventFeedConsumer&) = delete;
    EventFeedConsumer& operator=(const EventFeedConsumer&) = delete;

    [[nodiscard]] EventFeed& operator*() const;
    [[nodiscard]] EventFeed* operator->() const;

private:
    friend class EventFeedRegistry;
    explicit EventFeedConsumer(std::shared_ptr<EventFeed> feed);

    std::shared_ptr<EventFeed> feed;
};

/// Write access to a feed. Unregisters the feed when the handle dies, so that a worker which is torn down
/// and recreated inside one process, as the embedded backend does, does not collide with its own leftovers.
class EventFeedProducer
{
public:
    EventFeedProducer() = default;
    ~EventFeedProducer();

    EventFeedProducer(EventFeedProducer&& other) noexcept;
    EventFeedProducer& operator=(EventFeedProducer&& other) noexcept;
    EventFeedProducer(const EventFeedProducer&) = delete;
    EventFeedProducer& operator=(const EventFeedProducer&) = delete;

    [[nodiscard]] EventFeed& operator*() const;
    [[nodiscard]] EventFeed* operator->() const;

private:
    friend class EventFeedRegistry;
    EventFeedProducer(std::shared_ptr<EventFeed> feed, std::string host, std::string name);

    std::shared_ptr<EventFeed> feed;
    /// Kept as the raw string, so that the handle stays default constructible; 'Host' is not.
    std::string host;
    std::string name;
};

/// Process-global directory of feeds, keyed by the worker that owns a feed and the feed's name.
///
/// A source is constructed by the 'SourceRegistry' from nothing but its 'SourceDescriptor', so there is no
/// way to inject a worker-owned object into it. This registry closes that gap the same way 'NetworkSource'
/// does with the global receiver service. The host is part of the key because the embedded backend runs one
/// worker per topology node inside a single process, and those workers must not observe each other's events.
/// A source resolves the host from its descriptor, which is the same identity the optimizer places it on.
///
/// Registration is producer-driven: a consumer that could create feeds would turn a misspelled feed name
/// into a silently empty stream, which is indistinguishable from a feed that has nothing to report yet.
class EventFeedRegistry
{
public:
    static EventFeedRegistry& instance();

    EventFeedRegistry(const EventFeedRegistry&) = delete;
    EventFeedRegistry& operator=(const EventFeedRegistry&) = delete;
    EventFeedRegistry(EventFeedRegistry&&) = delete;
    EventFeedRegistry& operator=(EventFeedRegistry&&) = delete;

    /// Creates the feed that a producer publishes into. Throws if 'host' already registered 'name'.
    EventFeedProducer create(const Host& host, std::string_view name, size_t capacity);

    /// Exclusive read access to an existing feed. Prefers the feed of 'host', and falls back to the only
    /// feed of that name when the host does not match any, because a worker does not always know itself
    /// under the host its queries are placed on. Throws if no such feed exists, if several workers publish
    /// one and none of them is 'host', or if another consumer already holds it.
    EventFeedConsumer acquire(const Host& host, std::string_view name);

    /// Only used by tests, to keep the feeds of one test case out of the next.
    void clear();

private:
    friend class EventFeedProducer;

    EventFeedRegistry() = default;
    ~EventFeedRegistry() = default;

    void unregister(const std::string& host, const std::string& name);

    mutable std::mutex mutex;
    std::map<std::pair<std::string, std::string>, std::shared_ptr<EventFeed>> feeds;
};

}
