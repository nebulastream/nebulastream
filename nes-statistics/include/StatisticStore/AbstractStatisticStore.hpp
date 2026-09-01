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

#include <Identifiers/StatisticIdentifiers.hpp>
#include <Time/Timestamp.hpp>
#include <Statistic.hpp>

namespace NES
{

class AbstractStatisticStore
{
public:
    using IdStatisticPair = std::pair<StatisticId, Statistic>;

    AbstractStatisticStore() = default;
    virtual ~AbstractStatisticStore() = default;

    virtual bool insertStatistic(const StatisticId& statisticId, Statistic statistic) = 0;

    virtual bool deleteStatistics(const StatisticId& statisticId, const Timestamp& startTs, const Timestamp& endTs) = 0;

    virtual std::vector<Statistic> getStatistics(const StatisticId& statisticId, const Timestamp& startTs, const Timestamp& endTs) = 0;

    virtual std::optional<Statistic> getSingleStatistic(const StatisticId& statisticId, const Timestamp& startTs, const Timestamp& endTs)
        = 0;

    virtual std::vector<IdStatisticPair> getAllStatistics() = 0;
};

}
