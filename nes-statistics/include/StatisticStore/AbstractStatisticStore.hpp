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

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <Statistic.hpp>

namespace NES
{

enum class StatisticStoreType : uint8_t
{
    DEFAULT,
    WINDOW,
    SUB_STORES
};

class AbstractStatisticStore
{
public:
    using IdStatisticPair = std::pair<Statistic::StatisticId, Statistic>;

    AbstractStatisticStore() = default;
    virtual ~AbstractStatisticStore() = default;

    /// Inserts a statistic with the statisticId into a StatisticStore. Does not deduplicate: if multiple statistics are inserted with the
    /// same statisticId, startTs, and endTs, they all coexist in the store and there is no guarantee which of them is returned by
    /// getSingleStatistic, nor in what order they appear in getStatistics / getAllStatistics.
    virtual bool insertStatistic(const Statistic::StatisticId& statisticId, Statistic statistic) = 0;

    /// Deletes all statistics belonging to the statisticId in the period of [startTs, endTs]. Returns true, if any statistic was deleted
    virtual bool
    deleteStatistics(const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
        = 0;

    /// Gets all statistics belonging to the statisticId in the period of [startTs, endTs]
    virtual std::vector<Statistic>
    getStatistics(const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
        = 0;

    /// Gets a single statistic belonging to the statisticId that has exactly the startTs and endTs
    virtual std::optional<Statistic> getSingleStatistic(
        const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
        = 0;

    /// Returns all statistics which are currently saved in this store
    virtual std::vector<IdStatisticPair> getAllStatistics() = 0;
};

}
