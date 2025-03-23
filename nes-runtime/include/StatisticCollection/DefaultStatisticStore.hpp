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

#include <StatisticCollection/AbstractStatisticStore.hpp>
#include <folly/Synchronized.h>

namespace NES::Runtime
{

class DefaultStatisticStore final : public AbstractStatisticStore
{
public:
    /// Inserts a statistic with the statisticHash into a StatisticStore. Returns false, if statistic already exists
    bool insertStatistic(const StatisticHash& statisticHash, StatisticPtr statistic) override;

    /// Deletes all statistics belonging to the statisticHash in the period of [startTs, endTs]. Returns true, if any statistic was deleted
    bool deleteStatistics(
        const StatisticHash& statisticHash, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs) override;

    /// Gets all statistics belonging to the statisticHash in the period of [startTs, endTs]
    std::vector<StatisticPtr>
    getStatistics(const StatisticHash& statisticHash, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs) override;

    /// Gets a single statistic belonging to the statisticHash that has exactly the startTs and endTs
    std::optional<StatisticPtr> getSingleStatistic(
        const StatisticHash& statisticHash, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs) override;

    /// Returns all statistics which are currently saved in this store
    std::vector<HashStatisticPair> getAllStatistics() override;

private:
    folly::Synchronized<std::unordered_map<StatisticHash, std::vector<StatisticPtr>>> statistics;
};

}
