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

#include <StatisticStore/SubStoresStatisticStore.hpp>

#include <algorithm>
#include <iterator>
#include <ranges>

namespace NES
{
namespace
{
uint64_t getPos(const Statistic::StatisticHash& statisticHash, const uint64_t numberOfExpectedConcurrentAccess)
{
    /// We use here the randomness of the statisticHash to distribute the accesses.
    /// We can not use a worker thread id or etc, as this function is not only called from the execution
    const auto pos = statisticHash % numberOfExpectedConcurrentAccess;
    return pos;
}
}

SubStoresStatisticStore::SubStoresStatisticStore(const uint64_t numberOfExpectedConcurrentAccess)
    : numberOfExpectedConcurrentAccess(numberOfExpectedConcurrentAccess)
{
    allSubStores.reserve(numberOfExpectedConcurrentAccess);
    for (uint64_t i = 0; i < numberOfExpectedConcurrentAccess; ++i)
    {
        allSubStores.emplace_back(folly::Synchronized<std::vector<std::shared_ptr<Statistic>>>{});
    }
}

bool SubStoresStatisticStore::insertStatistic(const Statistic::StatisticHash& statisticHash, Statistic statistic)
{
    const auto pos = getPos(statisticHash, numberOfExpectedConcurrentAccess);
    const auto lockedStatisticStore = allSubStores[pos].wlock();
    lockedStatisticStore->emplace_back(std::make_shared<Statistic>(statistic));
    return true;
}

bool SubStoresStatisticStore::deleteStatistics(
    const Statistic::StatisticHash& statisticHash, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    const auto pos = getPos(statisticHash, numberOfExpectedConcurrentAccess);
    const auto lockedStatisticStore = allSubStores[pos].wlock();
    auto newIt = std::ranges::remove_if(
        *lockedStatisticStore,
        [startTs, endTs, statisticHash](const std::shared_ptr<Statistic>& statistic)
        { return statisticHash == statistic->getHash() and statistic->getStartTs() >= startTs and statistic->getEndTs() <= endTs; });

    const auto foundAnyStatistic = std::ranges::distance(newIt) > 0;
    if (foundAnyStatistic)
    {
        lockedStatisticStore->erase(newIt.begin(), newIt.end());
    }

    return foundAnyStatistic;
}

std::vector<std::shared_ptr<Statistic>> SubStoresStatisticStore::getStatistics(
    const Statistic::StatisticHash& statisticHash, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    const auto pos = getPos(statisticHash, numberOfExpectedConcurrentAccess);
    std::vector<std::shared_ptr<Statistic>> foundStatistics;
    const auto lockedStatisticStore = allSubStores[pos].rlock();
    std::ranges::copy_if(
        *lockedStatisticStore,
        std::back_inserter(foundStatistics),
        [startTs, endTs, statisticHash](const std::shared_ptr<Statistic>& statistic)
        {
            return statistic and statistic->getHash() == statisticHash and statistic->getStartTs() >= startTs
                and statistic->getEndTs() <= endTs;
        });
    return foundStatistics;
}

std::optional<std::shared_ptr<Statistic>> SubStoresStatisticStore::getSingleStatistic(
    const Statistic::StatisticHash& statisticHash, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    const auto pos = getPos(statisticHash, numberOfExpectedConcurrentAccess);
    std::vector<std::shared_ptr<Statistic>> foundStatistics;
    const auto lockedStatisticStore = allSubStores[pos].rlock();
    const auto foundStatistic = std::ranges::find_if(
        *lockedStatisticStore,
        [startTs, endTs, statisticHash](const std::shared_ptr<Statistic>& statistic)
        { return statistic->getHash() == statisticHash and statistic->getStartTs() >= startTs and statistic->getEndTs() <= endTs; });
    if (foundStatistic != lockedStatisticStore->end())
    {
        return *foundStatistic;
    }
    return {};
}

std::vector<AbstractStatisticStore::HashStatisticPair> SubStoresStatisticStore::getAllStatistics()
{
    std::vector<AbstractStatisticStore::HashStatisticPair> retStatistics;
    for (const auto& statisticStore : allSubStores)
    {
        const auto lockedStatisticStore = statisticStore.rlock();
        auto hashStatisticPairs = *lockedStatisticStore
            | std::views::transform([](const std::shared_ptr<Statistic>& statistic)
                                    { return std::make_pair(statistic->getHash(), std::move(statistic)); });
        std::ranges::copy(hashStatisticPairs, std::back_inserter(retStatistics));
    }
    return retStatistics;
}
}
