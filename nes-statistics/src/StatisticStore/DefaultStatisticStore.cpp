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

#include <StatisticStore/DefaultStatisticStore.hpp>

#include <limits>
#include <optional>
#include <ranges>
#include <utility>
#include <vector>
#include <Identifiers/StatisticIdentifiers.hpp>
#include <StatisticStore/AbstractStatisticStore.hpp>
#include <Time/Timestamp.hpp>
#include <StatisticTuple.hpp>

namespace NES
{

namespace
{
std::pair<std::pair<Timestamp, Timestamp>, std::pair<Timestamp, Timestamp>>
containedWindowBounds(const Timestamp& startTs, const Timestamp& endTs)
{
    constexpr auto lowest = std::numeric_limits<Timestamp::Underlying>::lowest();
    constexpr auto highest = std::numeric_limits<Timestamp::Underlying>::max();
    return {{startTs, Timestamp{lowest}}, {endTs, Timestamp{highest}}};
}
}

bool DefaultStatisticStore::insertStatistic(const StatisticId& statisticId, StatisticTuple statistic)
{
    const auto key = std::pair{statistic.getStartTs(), statistic.getEndTs()};
    const auto statisticsLocked = statistics.wlock();
    (*statisticsLocked)[statisticId].emplace(key, std::make_shared<const StatisticTuple>(std::move(statistic)));
    return true;
}

bool DefaultStatisticStore::deleteStatistics(const StatisticId& statisticId, const Timestamp& startTs, const Timestamp& endTs)
{
    const auto statisticsLocked = statistics.wlock();
    auto& windowed = (*statisticsLocked)[statisticId];
    const auto [lowerKey, upperKey] = containedWindowBounds(startTs, endTs);

    bool foundAny = false;
    const auto last = windowed.upper_bound(upperKey);
    for (auto it = windowed.lower_bound(lowerKey); it != last;)
    {
        if (it->first.second <= endTs)
        {
            it = windowed.erase(it);
            foundAny = true;
        }
        else
        {
            ++it;
        }
    }
    return foundAny;
}

std::vector<DefaultStatisticStore::StatisticRef>
DefaultStatisticStore::getStatistics(const StatisticId& statisticId, const Timestamp& startTs, const Timestamp& endTs)
{
    const auto statisticsLocked = statistics.rlock();
    const auto idIt = statisticsLocked->find(statisticId);
    if (idIt == statisticsLocked->end())
    {
        return {};
    }
    const auto [lowerKey, upperKey] = containedWindowBounds(startTs, endTs);

    std::vector<StatisticRef> foundStatistics;
    const auto last = idIt->second.upper_bound(upperKey);
    for (auto it = idIt->second.lower_bound(lowerKey); it != last; ++it)
    {
        if (it->first.second <= endTs)
        {
            foundStatistics.emplace_back(it->second);
        }
    }
    return foundStatistics;
}

std::optional<DefaultStatisticStore::StatisticRef>
DefaultStatisticStore::getSingleStatistic(const StatisticId& statisticId, const Timestamp& startTs, const Timestamp& endTs)
{
    const auto statisticsLocked = statistics.rlock();
    const auto idIt = statisticsLocked->find(statisticId);
    if (idIt == statisticsLocked->end())
    {
        return std::nullopt;
    }
    const auto it = idIt->second.find({startTs, endTs});
    return it != idIt->second.end() ? std::make_optional(it->second) : std::nullopt;
}

std::vector<DefaultStatisticStore::IdStatisticPair> DefaultStatisticStore::getAllStatistics()
{
    std::vector<IdStatisticPair> allStatistics;
    const auto statisticsLocked = statistics.rlock();
    for (const auto& [statisticId, windowed] : *statisticsLocked)
    {
        for (const auto& statistic : windowed | std::views::values)
        {
            allStatistics.emplace_back(statisticId, statistic);
        }
    }
    return allStatistics;
}

void DefaultStatisticStore::clear()
{
    statistics.wlock()->clear();
}

}
