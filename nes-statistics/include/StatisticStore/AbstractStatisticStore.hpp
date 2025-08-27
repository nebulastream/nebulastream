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
#include <vector>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <Statistic.hpp>

namespace NES
{

class AbstractStatisticStore
{
public:
    using HashStatisticPair = std::pair<Statistic::StatisticHash, std::shared_ptr<Statistic>>;

    AbstractStatisticStore() = default;
    virtual ~AbstractStatisticStore() = default;


    /// Inserts a statistic with the statisticHash into a StatisticStore. Returns false, if statistic already exists
    virtual bool insertStatistic(const Statistic::StatisticHash& statisticHash, Statistic statistic) = 0;

    /// Deletes all statistics belonging to the statisticHash in the period of [startTs, endTs]. Returns true, if any statistic was deleted
    virtual bool deleteStatistics(
        const Statistic::StatisticHash& statisticHash, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
        = 0;

    /// Gets all statistics belonging to the statisticHash in the period of [startTs, endTs]
    virtual std::vector<std::shared_ptr<Statistic>>
    getStatistics(const Statistic::StatisticHash& statisticHash, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
        = 0;

    /// Gets a single statistic belonging to the statisticHash that has exactly the startTs and endTs
    virtual std::optional<std::shared_ptr<Statistic>> getSingleStatistic(
        const Statistic::StatisticHash& statisticHash, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
        = 0;

    /// Returns all statistics which are currently saved in this store
    virtual std::vector<HashStatisticPair> getAllStatistics() = 0;
};

}
