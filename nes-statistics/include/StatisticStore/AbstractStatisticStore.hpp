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

#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include <Identifiers/StatisticIdentifiers.hpp>
#include <Time/Timestamp.hpp>
#include <StatisticTuple.hpp>

namespace NES
{

class AbstractStatisticStore
{
public:
    using StatisticRef = std::shared_ptr<const StatisticTuple>;
    using IdStatisticPair = std::pair<StatisticId, StatisticRef>;

    AbstractStatisticStore() = default;
    virtual ~AbstractStatisticStore() = default;
    AbstractStatisticStore(const AbstractStatisticStore&) = delete;
    AbstractStatisticStore& operator=(const AbstractStatisticStore&) = delete;
    AbstractStatisticStore(AbstractStatisticStore&&) = delete;
    AbstractStatisticStore& operator=(AbstractStatisticStore&&) = delete;

    /// Inserts a statistic with the statisticId into a StatisticStore. Does not deduplicate: if multiple statistics are inserted with the
    /// same statisticId, startTs, and endTs, they all coexist in the store and there is no guarantee which of them is returned by
    /// getSingleStatistic, nor in what order they appear in getStatistics / getAllStatistics.
    virtual bool insertStatistic(const StatisticId& statisticId, StatisticTuple statistic) = 0;

    /// Deletes all statistics belonging to the statisticId in the period of [startTs, endTs]. Returns true, if any statistic was deleted
    virtual bool deleteStatistics(const StatisticId& statisticId, const Timestamp& startTs, const Timestamp& endTs) = 0;

    /// Gets all statistics belonging to the statisticId in the period of [startTs, endTs]
    virtual std::vector<StatisticRef> getStatistics(const StatisticId& statisticId, const Timestamp& startTs, const Timestamp& endTs) = 0;

    /// Gets a single statistic belonging to the statisticId that has exactly the startTs and endTs
    virtual std::optional<StatisticRef> getSingleStatistic(const StatisticId& statisticId, const Timestamp& startTs, const Timestamp& endTs)
        = 0;

    /// Returns all statistics which are currently saved in this store
    virtual std::vector<IdStatisticPair> getAllStatistics() = 0;

    /// Deletes all statistics
    virtual void clear() = 0;
};

AbstractStatisticStore& globalStatisticStore();

}
