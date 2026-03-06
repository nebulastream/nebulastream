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

#include <StatisticStore/AbstractStatisticStore.hpp>
#include <folly/Synchronized.h>

namespace NES
{

/// Has multiple sub stores (std::vector) per expected no. concurrent access.
/// The main idea is that we use the statistic hash to distribute the access to the sub stores.
class SubStoresStatisticStore final : public AbstractStatisticStore
{
    uint64_t numberOfExpectedConcurrentAccess;
    std::vector<folly::Synchronized<std::vector<std::shared_ptr<Statistic>>>> allSubStores;

public:
    explicit SubStoresStatisticStore(uint64_t numberOfExpectedConcurrentAccess);
    bool insertStatistic(const Statistic::StatisticHash& statisticHash, Statistic statistic) override;
    bool deleteStatistics(
        const Statistic::StatisticHash& statisticHash, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs) override;
    std::vector<std::shared_ptr<Statistic>> getStatistics(
        const Statistic::StatisticHash& statisticHash, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs) override;
    std::optional<std::shared_ptr<Statistic>> getSingleStatistic(
        const Statistic::StatisticHash& statisticHash, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs) override;
    std::vector<HashStatisticPair> getAllStatistics() override;
};

}
