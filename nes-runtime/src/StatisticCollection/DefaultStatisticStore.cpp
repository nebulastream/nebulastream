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

#include <StatisticCollection/DefaultStatisticStore.hpp>

namespace NES::Runtime
{

bool DefaultStatisticStore::insertStatistic(const StatisticHash& statisticHash, StatisticPtr statistic)
{
    const auto statisticsLocked = statistics.wlock();
    auto& statisticsVec = (*statisticsLocked)[statisticHash];

    for (const auto& existingStatistic : statisticsVec)
    {
        if (statistic->getStartTs().equals(existingStatistic->getStartTs()) && statistic->getEndTs().equals(existingStatistic->getEndTs()))
        {
            return false;
        }
    }
    statisticsVec.emplace_back(statistic);
    return true;
}

bool DefaultStatisticStore::deleteStatistics(
    const StatisticHash& statisticHash, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    const auto statisticsLocked = statistics.wlock();
    auto& statisticsVec = (*statisticsLocked)[statisticHash];

    const auto range = std::ranges::remove_if(
        statisticsVec,
        [startTs, endTs](const StatisticPtr& statistic) { return startTs <= statistic->getStartTs() && statistic->getEndTs() <= endTs; });
    const bool foundAnyStatistic = range.begin() != statisticsVec.end();
    statisticsVec.erase(range.begin(), statisticsVec.end());
    return foundAnyStatistic;
}

std::vector<StatisticPtr> DefaultStatisticStore::getStatistics(
    const StatisticHash& statisticHash, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    std::vector<StatisticPtr> returnStatisticsVector;
    const auto statisticsLocked = statistics.rlock();
    auto& statisticsVec = statisticsLocked->at(statisticHash);

    std::ranges::copy_if(
        statisticsVec,
        std::back_inserter(returnStatisticsVector),
        [startTs, endTs](const StatisticPtr& statistic) { return startTs <= statistic->getStartTs() && statistic->getEndTs() <= endTs; });
    return returnStatisticsVector;
}

std::optional<StatisticPtr> DefaultStatisticStore::getSingleStatistic(
    const StatisticHash& statisticHash, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    const auto statisticsLocked = statistics.rlock();
    auto& statisticsVec = statisticsLocked->at(statisticHash);

    const auto it = std::ranges::find_if(
        statisticsVec,
        [startTs, endTs](const StatisticPtr& statistic) { return startTs == statistic->getStartTs() && statistic->getEndTs() == endTs; });
    return it != statisticsVec.end() ? std::make_optional(*it) : std::nullopt;
}

std::vector<HashStatisticPair> DefaultStatisticStore::getAllStatistics()
{
    std::vector<HashStatisticPair> returnStatisticsVector;
    const auto statisticsLocked = statistics.rlock();

    for (const auto& [statisticHash, statisticVec] : *statisticsLocked)
    {
        std::ranges::transform(
            statisticVec,
            std::back_inserter(returnStatisticsVector),
            [statisticHash](const StatisticPtr& statistic) { return std::make_pair(statisticHash, statistic); });
    }
    return returnStatisticsVector;
}

}
