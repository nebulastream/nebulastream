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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <unordered_map>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <folly/Synchronized.h>
#include <CollectionDomain.hpp>
#include <ConditionTrigger.hpp>
#include <Metric.hpp>
#include <Statistic.hpp>

namespace NES
{

/// Tracks which statistic requests are currently active (i.e., have a running query).
/// Deduplication is exact match: same metric, same collection domain, and same window size.
class StatisticRegistry
{
public:
    struct Key
    {
        Metric metric;
        CollectionDomain collectionDomain;
        Windowing::TimeMeasure windowSize;

        bool operator==(const Key& other) const
        {
            return metric == other.metric && collectionDomain == other.collectionDomain && windowSize == other.windowSize;
        }
    };

    struct KeyHash
    {
        size_t operator()(const Key& key) const
        {
            /// Fixed-point representation of the fractional part of the golden ratio to provide better distribution properties in hash functions
            static constexpr auto GoldenRatio = 0x9e3779b97f4a7c15;

            auto h = std::hash<size_t>{}(key.collectionDomain.index());
            h ^= std::visit([](const auto& d) -> size_t { return d.hash(); }, key.collectionDomain);
            h ^= std::hash<uint8_t>{}(static_cast<uint8_t>(key.metric)) + GoldenRatio + (h << 6) + (h >> 2);
            h ^= std::hash<uint64_t>{}(key.windowSize.getTime()) + GoldenRatio + (h << 6) + (h >> 2);
            return h;
        }
    };

    struct Entry
    {
        QueryId queryId;
        Statistic::StatisticId statisticId;
        std::vector<ConditionTrigger> triggers;
    };

    StatisticRegistry() = default;

    /// Returns existing entry if same key already registered.
    [[nodiscard]] std::optional<Entry> find(const Key& key) const;

    /// Registers a new statistic tracking entry, optionally with initial triggers.
    void registerStatistic(const Key& key, QueryId queryId, Statistic::StatisticId statisticId, std::vector<ConditionTrigger> triggers);

    /// Appends a condition trigger to an existing entry. Returns false if the key is not found.
    bool addTrigger(const Key& key, ConditionTrigger trigger);

    /// Removes the entry for this key. Returns true if an entry was removed.
    bool deregisterStatistic(const Key& key);

    size_t getNumberOfEntries();

    /// Iterates over all entries, calling the visitor for each one. Thread-safe.
    template <typename Fn>
    void forEachEntry(Fn&& visitor) const
    {
        for (const auto locked = registry.rlock(); const auto& [key, entry] : *locked)
        {
            visitor(key, entry);
        }
    }

private:
    folly::Synchronized<std::unordered_map<Key, Entry, KeyHash>> registry;
};

}
