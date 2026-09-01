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

#include <optional>
#include <utility>
#include <vector>
#include <Statistics/Statistic.hpp>
#include <Time/Timestamp.hpp>

namespace NES
{

/// Thread-safe store for statistics built by statistic build queries, owned by the worker and shared between the
/// build side (StatisticStoreWriter) and the probe side (StatisticStoreReader) of statistic queries.
class AbstractStatisticStore
{
public:
    using IdStatisticPair = std::pair<StatisticId, Statistic>;

    AbstractStatisticStore() = default;
    virtual ~AbstractStatisticStore() = default;

    /// Inserts a statistic with the statisticId into the store. Does not deduplicate: if multiple statistics are inserted with the
    /// same statisticId, startTs, and endTs, they all coexist in the store and there is no guarantee which of them is returned by
    /// getSingleStatistic, nor in what order they appear in getStatistics / getAllStatistics.
    virtual bool insertStatistic(StatisticId statisticId, Statistic statistic) = 0;

    /// Deletes all statistics belonging to the statisticId in the period of [startTs, endTs]. Returns true if any statistic was deleted.
    virtual bool deleteStatistics(StatisticId statisticId, Timestamp startTs, Timestamp endTs) = 0;

    /// Gets all statistics belonging to the statisticId in the period of [startTs, endTs]
    virtual std::vector<Statistic> getStatistics(StatisticId statisticId, Timestamp startTs, Timestamp endTs) = 0;

    /// Gets a single statistic belonging to the statisticId that has exactly the startTs and endTs.
    /// The returned statistic shares ownership of the statistic data, so its data pointer stays valid independently of the store.
    virtual std::optional<Statistic> getSingleStatistic(StatisticId statisticId, Timestamp startTs, Timestamp endTs) = 0;

    /// Returns all statistics which are currently saved in this store
    virtual std::vector<IdStatisticPair> getAllStatistics() = 0;
};

}
