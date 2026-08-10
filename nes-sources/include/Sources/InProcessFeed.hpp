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
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <folly/MPMCQueue.h>

namespace NES
{

/// A bounded channel of text rows between a producer living somewhere else in the worker process and an
/// 'InProcessSource' instance that is created later, when a query that reads the feed is started.
///
/// Rows are opaque to the feed. A producer is expected to emit whatever the source's InputFormatter parses,
/// i.e., one formatted tuple per row without the tuple delimiter (the source appends it).
///
/// Producers must never block, because they may run on latency critical threads such as the query engine's
/// worker threads. 'tryPush' therefore drops the row when the feed is full and counts the drop instead.
class InProcessFeed
{
public:
    explicit InProcessFeed(size_t capacity);

    /// Non-blocking. Returns false if the row was dropped because the feed is full.
    bool tryPush(std::string row);

    /// Waits at most 'timeout' for a row to become available.
    std::optional<std::string> tryPop(std::chrono::milliseconds timeout);

    [[nodiscard]] size_t droppedRows() const;
    [[nodiscard]] size_t capacity() const;

private:
    folly::MPMCQueue<std::string> queue;
    std::atomic<size_t> dropped{0};
};

/// Process-global directory of feeds, keyed by name.
///
/// A 'Source' is constructed by the 'SourceRegistry' from nothing but its 'SourceDescriptor', so there is no
/// way to inject a worker-owned object into it. This registry closes that gap the same way 'NetworkSource'
/// does with the global receiver service: the producer creates a named feed during worker startup, and the
/// source looks the feed up by the name it reads from its descriptor.
class InProcessFeedRegistry
{
public:
    static InProcessFeedRegistry& instance();

    InProcessFeedRegistry(const InProcessFeedRegistry&) = delete;
    InProcessFeedRegistry& operator=(const InProcessFeedRegistry&) = delete;
    InProcessFeedRegistry(InProcessFeedRegistry&&) = delete;
    InProcessFeedRegistry& operator=(InProcessFeedRegistry&&) = delete;

    /// Returns the feed registered under 'name', creating it with 'capacity' if it does not exist yet.
    /// Both the producer and the source call this, so neither has to be started first. 'capacity' is
    /// ignored if the feed already exists.
    std::shared_ptr<InProcessFeed> getOrCreate(const std::string& name, size_t capacity);

    /// Only used by tests, to keep feeds of one test case out of the next one.
    void clear();

private:
    InProcessFeedRegistry() = default;
    ~InProcessFeedRegistry() = default;

    mutable std::mutex mutex;
    std::unordered_map<std::string, std::shared_ptr<InProcessFeed>> feeds;
};

}
