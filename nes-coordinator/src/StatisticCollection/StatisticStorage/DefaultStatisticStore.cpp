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

#include <Operators/LogicalOperators/Windows/Measures/TimeMeasure.hpp>
#include <StatisticCollection/StatisticStorage/DefaultStatisticStore.hpp>
namespace NES::Statistic {

AbstractStatisticStorePtr DefaultStatisticStore::create() { return std::make_shared<DefaultStatisticStore>(); }

std::vector<StatisticPtr> DefaultStatisticStore::getStatistics(const StatisticHash& statisticHash,
                                                               const Windowing::TimeMeasure& startTs,
                                                               const Windowing::TimeMeasure& endTs) {
    auto lockedKeyToStatisticMap = keyToStatistics.wlock();
    auto& statisticVec = (*lockedKeyToStatisticMap)[statisticHash];
    std::vector<StatisticPtr> returnStatisticsVector;

    /* We describe a bool lambda that checks if the statistics lies between [startTs, endTs].
     * Afterward, we use the lambda function in the copy_if() that iterates over the statisticVec and copies
     * all items, that satisfy the lambda condition.
     */
    auto getCondition = [startTs, endTs](const StatisticPtr& statistic) {
        return startTs <= statistic->getStartTs() && statistic->getEndTs() <= endTs;
    };
    std::copy_if(statisticVec.begin(), statisticVec.end(), std::back_inserter(returnStatisticsVector), getCondition);
    return returnStatisticsVector;
}

bool DefaultStatisticStore::insertStatistic(const StatisticHash& statisticHash, StatisticPtr statistic) {
    auto lockedKeyToStatisticMap = keyToStatistics.wlock();
    auto& statisticVec = (*lockedKeyToStatisticMap)[statisticHash];

    // For now, we do not allow duplicate statistics. Meaning the same statistic key with the same startTs and endTs
    for (const auto& stat : statisticVec) {
        if (statistic->getStartTs().equals(stat->getStartTs()) && statistic->getEndTs().equals(stat->getEndTs())) {
            return false;
        }
    }

    statisticVec.emplace_back(statistic);
    return true;
}

bool DefaultStatisticStore::deleteStatistics(const StatisticHash& statisticHash,
                                             const Windowing::TimeMeasure& startTs,
                                             const Windowing::TimeMeasure& endTs) {
    auto lockedKeyToStatisticMap = keyToStatistics.wlock();
    auto& statisticVec = (*lockedKeyToStatisticMap)[statisticHash];

    auto deleteCondition = [startTs, endTs](const StatisticPtr& statistic) {
        return startTs <= statistic->getStartTs() && statistic->getEndTs() <= endTs;
    };
    auto removeBeginIt = std::remove_if(statisticVec.begin(), statisticVec.end(), deleteCondition);
    const bool foundAnyStatistic = removeBeginIt != statisticVec.end();
    statisticVec.erase(removeBeginIt, statisticVec.end());
    return foundAnyStatistic;
}

DefaultStatisticStore::~DefaultStatisticStore() = default;
}// namespace NES::Statistic