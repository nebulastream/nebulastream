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

#include <StatisticCollection/StatisticStorage/BuildStatisticStore.hpp>

namespace NES::Statistic {

StatisticStorePtr BuildStatisticStore::create() {
    return std::make_shared<BuildStatisticStore>();
}

std::vector<StatisticPtr> BuildStatisticStore::getStatistics(const StatisticHash& statisticHash,
                                                             const Windowing::TimeMeasure& startTs,
                                                             const Windowing::TimeMeasure& endTs) {
    auto lockedKeyToStatisticMap = statisticHashToStatistics.wlock();
    auto& statisticMap = (*lockedKeyToStatisticMap)[statisticHash];

    std::vector<StatisticPtr> returnStatisticsVector;
    for (const auto& [mapEndTs, statistic] : statisticMap) {
        if (startTs <= statistic->getStartTs() && statistic->getEndTs() <= endTs) {
            returnStatisticsVector.emplace_back(statistic);
        }
    }
    return returnStatisticsVector;
}

std::optional<StatisticPtr> BuildStatisticStore::getSingleStatistic(const StatisticHash& statisticHash,
                                                                    const Windowing::TimeMeasure&,
                                                                    const Windowing::TimeMeasure& endTs) {

    auto lockedKeyToStatisticMap = statisticHashToStatistics.wlock();
    auto& statisticMap = (*lockedKeyToStatisticMap)[statisticHash];
    if (statisticMap.find(endTs.getTime()) != statisticMap.end()) {
        return statisticMap[endTs.getTime()];
    } else {
        return {};
    }
}

std::vector<HashStatisticPair> BuildStatisticStore::getAllStatistics() {
    std::vector<HashStatisticPair> allStatistics;
    auto lockedKeyToStatisticMap = statisticHashToStatistics.wlock();
    for (const auto& [statisticHash, statisticMap] : *lockedKeyToStatisticMap) {
        for (const auto& [endTs, statistic] : statisticMap) {
            allStatistics.emplace_back(statisticHash, statistic);
        }
    }
    return allStatistics;
}

bool BuildStatisticStore::insertStatistic(const StatisticHash& statisticHash, StatisticPtr statistic) {
    auto lockedKeyToStatisticMap = statisticHashToStatistics.wlock();
    auto& statisticMap = (*lockedKeyToStatisticMap)[statisticHash];

    // We simply override a statistic that has the same end ts and the same statisticHash, as this should never happen
    statisticMap[statistic->getEndTs().getTime()] = statistic;
    return true;
}

bool BuildStatisticStore::deleteStatistics(const StatisticHash& statisticHash,
                                           const Windowing::TimeMeasure& startTs,
                                           const Windowing::TimeMeasure& endTs) {
    auto lockedKeyToStatisticMap = statisticHashToStatistics.wlock();
    auto& statisticMap = (*lockedKeyToStatisticMap)[statisticHash];

    bool foundAnyStatistic = false;
    for (auto it = statisticMap.begin(); it != statisticMap.end();) {
        const auto& statistic = it->second;
        if (startTs <= statistic->getStartTs() && statistic->getEndTs() <= endTs) {
            it = statisticMap.erase(it);
            foundAnyStatistic = true;
        } else {
            ++it;
        }
    }
    return foundAnyStatistic;
}

BuildStatisticStore::~BuildStatisticStore() = default;
}// namespace NES::Statistic