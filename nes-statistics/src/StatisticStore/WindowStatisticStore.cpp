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

#include <StatisticStore/WindowStatisticStore.hpp>

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

WindowStatisticStore::WindowStatisticStore(const uint64_t numberOfExpectedConcurrentAccess, const Windowing::TimeMeasure windowSize)
    : numberOfExpectedConcurrentAccess(numberOfExpectedConcurrentAccess), windowSize(windowSize)
{
    allStatistics.reserve(numberOfExpectedConcurrentAccess);
    for (uint64_t i = 0; i < numberOfExpectedConcurrentAccess; ++i)
    {
        allStatistics.emplace_back(
            folly::Synchronized<std::unordered_map<StatisticKey, std::vector<std::shared_ptr<Statistic>>, StatisticKeyHash>>{});
    }
}

Windowing::TimeMeasure WindowStatisticStore::calculateWindowStartTime(const Windowing::TimeMeasure statStartTime) const
{
    const auto startTimeRawValue = statStartTime.getTime();
    const uint64_t windowStartTime = windowSize.getTime() * std::floor((startTimeRawValue / windowSize.getTime()));
    return Windowing::TimeMeasure{windowStartTime};
}

bool WindowStatisticStore::insertStatistic(const Statistic::StatisticHash& statisticHash, Statistic statistic)
{
    const auto pos = getPos(statisticHash, numberOfExpectedConcurrentAccess);
    const auto windowStartTime = calculateWindowStartTime(statistic.getStartTs());
    const auto lockedStatisticStore = allStatistics[pos].wlock();
    (*lockedStatisticStore)[{statisticHash, windowStartTime}].emplace_back(std::make_shared<Statistic>(statistic));
    return true;
}

bool WindowStatisticStore::deleteStatistics(
    const Statistic::StatisticHash& statisticHash, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    const auto pos = getPos(statisticHash, numberOfExpectedConcurrentAccess);
    const uint64_t firstWindow = std::floor(startTs.getTime() / windowSize.getTime());
    const uint64_t lastWindow = std::floor(endTs.getTime() / windowSize.getTime());
    const auto numberOfWindows = lastWindow - firstWindow + 1;
    bool foundAnyStatistic = false;

    for (uint64_t i = 0; i < numberOfWindows; ++i)
    {
        auto lockedStatisticStore = allStatistics[pos].wlock();
        const Windowing::TimeMeasure curWindowStartTime{firstWindow * windowSize.getTime() + i * windowSize.getTime()};
        auto& window = (*lockedStatisticStore)[{statisticHash, curWindowStartTime}];
        auto newEnd = std::ranges::remove_if(
            window,
            [startTs, endTs](const std::shared_ptr<Statistic>& curStatistic)
            { return curStatistic->getStartTs() >= startTs and curStatistic->getEndTs() <= endTs; });

        foundAnyStatistic |= (newEnd.begin() != window.end());
        if (newEnd.begin() != window.end())
        {
            window.erase(newEnd.begin(), newEnd.end());
        }
    }

    return foundAnyStatistic;
}

std::vector<std::shared_ptr<Statistic>> WindowStatisticStore::getStatistics(
    const Statistic::StatisticHash& statisticHash, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    const auto pos = getPos(statisticHash, numberOfExpectedConcurrentAccess);
    const uint64_t firstWindow = std::floor(startTs.getTime() / windowSize.getTime());
    const uint64_t lastWindow = std::floor(endTs.getTime() / windowSize.getTime());
    const auto numberOfWindows = lastWindow - firstWindow + 1;
    std::vector<std::shared_ptr<Statistic>> foundStatistics;

    for (uint64_t i = 0; i < numberOfWindows; ++i)
    {
        const auto lockedStatisticStore = allStatistics[pos].rlock();
        const Windowing::TimeMeasure curWindowStartTime{firstWindow * windowSize.getTime() + i * windowSize.getTime()};
        if (auto window = lockedStatisticStore->find(StatisticKey{.statisticHash = statisticHash, .startTs = curWindowStartTime});
            window != lockedStatisticStore->end())
        {
            std::ranges::copy_if(
                window->second,
                std::back_inserter(foundStatistics),
                [startTs, endTs](const std::shared_ptr<Statistic>& curStatistic)
                { return curStatistic->getStartTs() >= startTs and curStatistic->getEndTs() <= endTs; });
        }
    }

    return foundStatistics;
}

std::optional<std::shared_ptr<Statistic>> WindowStatisticStore::getSingleStatistic(
    const Statistic::StatisticHash& statisticHash, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    const auto pos = getPos(statisticHash, numberOfExpectedConcurrentAccess);
    const uint64_t firstWindow = std::floor(startTs.getTime() / windowSize.getTime());

    const auto lockedStatisticStore = allStatistics[pos].rlock();
    const Windowing::TimeMeasure curWindowStartTime{firstWindow * windowSize.getTime()};
    auto& window = lockedStatisticStore->at({statisticHash, curWindowStartTime});
    const auto foundStatistic = std::ranges::find_if(
        window,
        [startTs, endTs](const std::shared_ptr<Statistic>& curStatistic)
        { return curStatistic->getStartTs() >= startTs and curStatistic->getEndTs() <= endTs; });
    if (foundStatistic != window.end())
    {
        return *foundStatistic;
    }

    return {};
}

std::vector<AbstractStatisticStore::HashStatisticPair> WindowStatisticStore::getAllStatistics()
{
    std::vector<AbstractStatisticStore::HashStatisticPair> retStatistics;
    for (auto& statisticStore : allStatistics)
    {
        auto lockedStatisticStore = statisticStore.rlock();
        for (auto& [key, window] : *lockedStatisticStore)
        {
            auto hashStatisticPairs = window
                | std::views::transform([&key](const std::shared_ptr<Statistic>& statistic)
                                        { return std::make_pair(key.statisticHash, std::move(statistic)); });
            std::ranges::copy(hashStatisticPairs, std::back_inserter(retStatistics));
        }
    }

    return retStatistics;
}

}
