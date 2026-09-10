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

#include <map>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/StatisticIdentifiers.hpp>
#include <StatisticStore/AbstractStatisticStore.hpp>
#include <Time/Timestamp.hpp>
#include <folly/Synchronized.h>
#include <StatisticTuple.hpp>

namespace NES
{

class DefaultStatisticStore final : public AbstractStatisticStore
{
public:
    bool insertStatistic(const StatisticId& statisticId, StatisticTuple statistic) override;
    bool deleteStatistics(const StatisticId& statisticId, const Timestamp& startTs, const Timestamp& endTs) override;
    std::vector<StatisticRef> getStatistics(const StatisticId& statisticId, const Timestamp& startTs, const Timestamp& endTs) override;
    std::optional<StatisticRef>
    getSingleStatistic(const StatisticId& statisticId, const Timestamp& startTs, const Timestamp& endTs) override;
    std::vector<IdStatisticPair> getAllStatistics() override;
    void clear() override;

private:
    using WindowedStatistics = std::multimap<std::pair<Timestamp, Timestamp>, StatisticRef>;

    folly::Synchronized<std::unordered_map<StatisticId, WindowedStatistics>> statistics;
};

}
