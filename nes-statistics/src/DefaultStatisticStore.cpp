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

#include <Statistics/DefaultStatisticStore.hpp>

#include <algorithm>
#include <iterator>
#include <optional>
#include <vector>
#include <Time/Timestamp.hpp>
#include <StatisticTuple.hpp>

namespace NES
{

bool DefaultStatisticStore::insertStatistic(const StatisticId& statisticId, StatisticTuple statistic)
{
    const auto statisticsLocked = statistics.wlock();
    (*statisticsLocked)[statisticId].emplace_back(std::move(statistic));
    return true;
}

bool DefaultStatisticStore::deleteStatistics(const StatisticId& statisticId, const Timestamp& startTs, const Timestamp& endTs)
{
    const auto statisticsLocked = statistics.wlock();
    auto& statisticsVec = (*statisticsLocked)[statisticId];

    std::vector<StatisticTuple> kept;
    kept.reserve(statisticsVec.size());
    for (auto& statistic : statisticsVec)
    {
        if (not(startTs <= statistic.getStartTs() and statistic.getEndTs() <= endTs))
        {
            kept.emplace_back(std::move(statistic));
        }
    }
    const bool foundAny = kept.size() != statisticsVec.size();
    statisticsVec = std::move(kept);
    return foundAny;
}

std::vector<StatisticTuple>
DefaultStatisticStore::getStatistics(const StatisticId& statisticId, const Timestamp& startTs, const Timestamp& endTs)
{
    const auto statisticsLocked = statistics.rlock();
    const auto idIt = statisticsLocked->find(statisticId);
    if (idIt == statisticsLocked->end())
    {
        return {};
    }

    std::vector<StatisticTuple> foundStatistics;
    std::ranges::copy_if(
        idIt->second,
        std::back_inserter(foundStatistics),
        [startTs, endTs](const StatisticTuple& statistic) { return startTs <= statistic.getStartTs() and statistic.getEndTs() <= endTs; });
    return foundStatistics;
}

std::optional<StatisticTuple>
DefaultStatisticStore::getSingleStatistic(const StatisticId& statisticId, const Timestamp& startTs, const Timestamp& endTs)
{
    const auto statisticsLocked = statistics.rlock();
    const auto idIt = statisticsLocked->find(statisticId);
    if (idIt == statisticsLocked->end())
    {
        return std::nullopt;
    }
    const auto& statisticsVec = idIt->second;

    const auto it = std::ranges::find_if(
        statisticsVec,
        [startTs, endTs](const StatisticTuple& statistic) { return startTs == statistic.getStartTs() and statistic.getEndTs() == endTs; });
    return it != statisticsVec.end() ? std::make_optional(*it) : std::nullopt;
}

std::vector<DefaultStatisticStore::IdStatisticPair> DefaultStatisticStore::getAllStatistics()
{
    std::vector<IdStatisticPair> allStatistics;
    const auto statisticsLocked = statistics.rlock();
    for (const auto& [statisticId, statisticsVec] : *statisticsLocked)
    {
        for (const auto& statistic : statisticsVec)
        {
            allStatistics.emplace_back(statisticId, statistic);
        }
    }
    return allStatistics;
}

}
