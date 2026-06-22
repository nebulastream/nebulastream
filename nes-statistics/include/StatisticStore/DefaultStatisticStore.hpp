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
#include <unordered_map>
#include <vector>
#include <StatisticStore/AbstractStatisticStore.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <folly/Synchronized.h>
#include <Statistic.hpp>

namespace NES
{

class DefaultStatisticStore final : public AbstractStatisticStore
{
public:
    bool insertStatistic(const Statistic::StatisticId& statisticId, Statistic statistic) override;
    bool deleteStatistics(
        const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs) override;
    std::vector<Statistic> getStatistics(
        const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs) override;
    std::optional<Statistic> getSingleStatistic(
        const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs) override;
    std::vector<IdStatisticPair> getAllStatistics() override;

private:
    folly::Synchronized<std::unordered_map<Statistic::StatisticId, std::vector<Statistic>>> statistics;
};

}
