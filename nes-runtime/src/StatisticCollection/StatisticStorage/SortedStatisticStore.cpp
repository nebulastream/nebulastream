#include <Measures/TimeMeasure.hpp>
#include <StatisticCollection/StatisticStorage/SortedStatisticStore.hpp>
#include <set>
namespace NES::Statistic {

StatisticStorePtr SortedStatisticStore::create() { return std::make_shared<SortedStatisticStore>(); }

std::vector<StatisticPtr> SortedStatisticStore::getStatistics(const StatisticHash& statisticHash,
                                                           const Windowing::TimeMeasure& startTs,
                                                           const Windowing::TimeMeasure& endTs) {
    auto lockedKeyToStatisticMap = keyToStatistics.wlock();
    auto& statisticSet = (*lockedKeyToStatisticMap)[statisticHash];
    std::vector<StatisticPtr> returnStatisticsVector;

    // Binary search to find the range of statistics within the specified time range
    auto startIt = std::lower_bound(statisticSet.begin(), statisticSet.end(), startTs,
                                     [](const StatisticPtr& stat, const Windowing::TimeMeasure& ts) {
                                         return stat->getStartTs() < ts;
                                     });

    auto endIt = std::upper_bound(statisticSet.begin(), statisticSet.end(), endTs,
                                   [](const Windowing::TimeMeasure& ts, const StatisticPtr& stat) {
                                       return ts < stat->getEndTs();
                                   });

    // Copy the statistics within the range to the return vector
    for (auto it = startIt; it != endIt; ++it) {
        if ((*it)->getEndTs() <= endTs) {
            returnStatisticsVector.push_back(*it);
        }
    }

    return returnStatisticsVector;
}

std::optional<StatisticPtr> SortedStatisticStore::getSingleStatistic(const StatisticHash& statisticHash,
                                                                  const Windowing::TimeMeasure& startTs,
                                                                  const Windowing::TimeMeasure& endTs) {
    auto lockedKeyToStatisticMap = keyToStatistics.wlock();
    auto& statisticSet = (*lockedKeyToStatisticMap)[statisticHash];

    // Binary search to find the range of statistics within the specified time range
    auto startIt = std::lower_bound(statisticSet.begin(), statisticSet.end(), startTs,
                                     [](const StatisticPtr& stat, const Windowing::TimeMeasure& ts) {
                                         return stat->getStartTs() < ts;
                                     });

    auto endIt = std::upper_bound(statisticSet.begin(), statisticSet.end(), endTs,
                                   [](const Windowing::TimeMeasure& ts, const StatisticPtr& stat) {
                                       return ts < stat->getEndTs();
                                   });

    // Check if there's any statistic within the range
    if (startIt != endIt && (*startIt)->getEndTs() <= endTs) {
        return *startIt;
    }

    return std::nullopt; // No statistic found within the range
}

bool SortedStatisticStore::insertStatistic(const StatisticHash& statisticHash, StatisticPtr statistic) {
    auto lockedKeyToStatisticMap = keyToStatistics.wlock();
    auto& statisticSet = (*lockedKeyToStatisticMap)[statisticHash];

    // Binary search to find the insertion point
    auto insertionPoint = std::lower_bound(statisticSet.begin(), statisticSet.end(), statistic,
                                            [](const StatisticPtr& a, const StatisticPtr& b) {
                                                return a->getStartTs() < b->getStartTs();
                                            });

    // Check if the statistic already exists
    if (insertionPoint != statisticSet.end() && (*insertionPoint)->getStartTs().equals(statistic->getStartTs())
        && (*insertionPoint)->getEndTs().equals(statistic->getEndTs())) {
        return false; // Duplicate found
    }

    // Insert the statistic at the found insertion point
    statisticSet.insert(insertionPoint, statistic);
    return true;
}

bool SortedStatisticStore::deleteStatistics(const StatisticHash& statisticHash,
                                         const Windowing::TimeMeasure& startTs,
                                         const Windowing::TimeMeasure& endTs) {
    auto lockedKeyToStatisticMap = keyToStatistics.wlock();
    auto& statisticSet = (*lockedKeyToStatisticMap)[statisticHash];

    // Binary search to find the range of statistics within the specified time range
    auto startIt = std::lower_bound(statisticSet.begin(), statisticSet.end(), startTs,
                                     [](const StatisticPtr& stat, const Windowing::TimeMeasure& ts) {
                                         return stat->getStartTs() < ts;
                                     });

    auto endIt = std::upper_bound(statisticSet.begin(), statisticSet.end(), endTs,
                                   [](const Windowing::TimeMeasure& ts, const StatisticPtr& stat) {
                                       return ts < stat->getEndTs();
                                   });

    // Erase the statistics within the range
    auto it = startIt;
    bool foundAnyStatistic = false;
    while (it != endIt) {
        it = statisticSet.erase(it);
        foundAnyStatistic = true;
    }

    return foundAnyStatistic;
}

std::vector<HashStatisticPair> SortedStatisticStore::getAllStatistics() {
    auto lockedKeyToStatisticMap = keyToStatistics.wlock();
    std::vector<HashStatisticPair> returnStatisticsVector;

    for (const auto& [statisticHash, statisticSet] : *lockedKeyToStatisticMap) {
        for (const auto& stat : statisticSet) {
            returnStatisticsVector.emplace_back(statisticHash, stat);
        }
    }
    return returnStatisticsVector;
}

SortedStatisticStore::~SortedStatisticStore() = default;
}// namespace NES::Statistic