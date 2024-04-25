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

#include <StatisticCollection/StatisticCache/AbstractStatisticCache.hpp>
#include <StatisticCollection/StatisticCache/DefaultStatisticCache.hpp>
#include <Util/Logger/Logger.hpp>
namespace NES::Statistic {

StatisticCachePtr DefaultStatisticCache::create() { return std::make_shared<DefaultStatisticCache>(); }

bool DefaultStatisticCache::insertStatistic(const StatisticHash& statisticHash, const StatisticValue<>& statisticValue) {
    auto lockedKeyToStatisticMap = keyToStatistics.wlock();
    auto& statisticVec = (*lockedKeyToStatisticMap)[statisticHash];

    // For now, we do not allow duplicate statistics. Meaning the same statistic key with the same startTs and endTs
    const auto startTs = statisticValue.getStartTs();
    const auto endTs = statisticValue.getEndTs();
    for (const auto& stat : statisticVec) {
        if (startTs.equals(stat->getStartTs()) && endTs.equals(stat->getEndTs())) {
            NES_ERROR("Duplicate statistic key with the same startTs {} and endTs {}", startTs.toString(), endTs.toString());
            return false;
        }
    }

    statisticVec.emplace_back(
        std::make_shared<StatisticValue<>>(statisticValue.getValue(), statisticValue.getStartTs(), statisticValue.getEndTs()));
    return true;
}

std::vector<StatisticValuePtr> DefaultStatisticCache::getStatistic(const StatisticHash& statisticHash,
                                                                   const Windowing::TimeMeasure& startTs,
                                                                   const Windowing::TimeMeasure& endTs) {
    auto lockedKeyToStatisticMap = keyToStatistics.wlock();
    auto& statisticVec = (*lockedKeyToStatisticMap)[statisticHash];
    std::vector<StatisticValuePtr> returnStatisticsVector;

    /* We describe a bool lambda that checks if the statistics lies between [startTs, endTs].
     * Afterward, we use the lambda function in the copy_if() that iterates over the statisticVec and copies
     * all items, that satisfy the lambda condition.
     */
    auto getCondition = [startTs, endTs](const StatisticValuePtr& statistic) {
        return startTs <= statistic->getStartTs() && statistic->getEndTs() <= endTs;
    };
    std::copy_if(statisticVec.begin(), statisticVec.end(), std::back_inserter(returnStatisticsVector), getCondition);
    return returnStatisticsVector;
}

bool DefaultStatisticCache::deleteStatistics(const StatisticHash& statisticHash,
                                             const Windowing::TimeMeasure& startTs,
                                             const Windowing::TimeMeasure& endTs) {
    auto lockedKeyToStatisticMap = keyToStatistics.wlock();
    auto& statisticVec = (*lockedKeyToStatisticMap)[statisticHash];

    auto deleteCondition = [startTs, endTs](const StatisticValuePtr& statistic) {
        return startTs <= statistic->getStartTs() && statistic->getEndTs() <= endTs;
    };

    auto removeBeginIt = std::remove_if(statisticVec.begin(), statisticVec.end(), deleteCondition);
    const bool foundAnyStatistic = removeBeginIt != statisticVec.end();
    statisticVec.erase(removeBeginIt, statisticVec.end());
    return foundAnyStatistic;
}
}// namespace NES::Statistic