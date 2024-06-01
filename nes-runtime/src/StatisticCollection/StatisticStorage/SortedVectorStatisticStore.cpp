#include <Measures/TimeMeasure.hpp>
#include <StatisticCollection/StatisticStorage/SortedVectorStatisticStore.hpp>
#include <set>
namespace NES::Statistic {

StatisticStorePtr SortedVectorStatisticStore::create() { return std::make_shared<SortedVectorStatisticStore>(); }

std::vector<StatisticPtr> SortedVectorStatisticStore::getStatistics(const StatisticHash& statisticHash,
                                                               const Windowing::TimeMeasure& startTs,
                                                               const Windowing::TimeMeasure& endTs) {
    auto lockedKeyToStatisticMap = keyToStatistics.wlock();
    auto& statisticSet = (*lockedKeyToStatisticMap)[statisticHash];
    std::vector<StatisticPtr> returnStatisticsVector;

    auto getCondition = [startTs, endTs](const StatisticPtr& statistic) {
        return startTs <= statistic->getStartTs() && statistic->getEndTs() <= endTs;
    };

    for (const auto& stat : statisticSet) {
        if (getCondition(stat)) {
            returnStatisticsVector.push_back(stat);
        }
    }
    return returnStatisticsVector;
}

std::optional<StatisticPtr> SortedVectorStatisticStore::getSingleStatistic(const StatisticHash& statisticHash,
                                                                           const Windowing::TimeMeasure& startTs,
                                                                           const Windowing::TimeMeasure& endTs) {
    auto lockedKeyToStatisticMap = keyToStatistics.wlock();
    auto it = lockedKeyToStatisticMap->find(statisticHash);

    // Check if the statisticHash exists in the map
    if (it != lockedKeyToStatisticMap->end() && !it->second.empty()) {
        // Search for a statistic that matches the given time range
        for (const auto& statistic : it->second) {
            if (statistic->getStartTs() == startTs && statistic->getEndTs() == endTs) {
                // If a statistic with the specified time range is found, return it
                return statistic;
            }
        }
    }

    // If no matching statistic is found, return std::nullopt
    return std::nullopt;
}

bool SortedVectorStatisticStore::insertStatistic(const StatisticHash& statisticHash, StatisticPtr statistic) {
    auto lockedKeyToStatisticMap = keyToStatistics.wlock();
    auto& statisticSet = (*lockedKeyToStatisticMap)[statisticHash];

    // For now, we do not allow duplicate statistics. Meaning the same statistic key with the same startTs and endTs
    for (const auto& stat : statisticSet) {
        if (statistic->getStartTs().equals(stat->getStartTs()) && statistic->getEndTs().equals(stat->getEndTs())) {
            return false; // Duplicate found
        }
    }

    statisticSet.push_back(statistic);
    // Resort the vector after insertion
    std::sort(statisticSet.begin(), statisticSet.end(), [](const StatisticPtr& a, const StatisticPtr& b) {
        return a->getStartTs() < b->getStartTs();
    });

    return true;
}

bool SortedVectorStatisticStore::deleteStatistics(const StatisticHash& statisticHash,
                                             const Windowing::TimeMeasure& startTs,
                                             const Windowing::TimeMeasure& endTs) {
    auto lockedKeyToStatisticMap = keyToStatistics.wlock();
    auto& statisticSet = (*lockedKeyToStatisticMap)[statisticHash];

    auto deleteCondition = [startTs, endTs](const StatisticPtr& statistic) {
        return startTs <= statistic->getStartTs() && statistic->getEndTs() <= endTs;
    };

    auto it = statisticSet.begin();
    bool foundAnyStatistic = false;
    while (it != statisticSet.end()) {
        if (deleteCondition(*it)) {
            it = statisticSet.erase(it);
            foundAnyStatistic = true;
        } else {
            ++it;
        }
    }
    return foundAnyStatistic;

}

std::vector<HashStatisticPair> SortedVectorStatisticStore::getAllStatistics() {
    auto lockedKeyToStatisticMap = keyToStatistics.wlock();
    std::vector<HashStatisticPair> returnStatisticsVector;

    for (const auto& [statisticHash, statisticSet] : *lockedKeyToStatisticMap) {
        for (const auto& stat : statisticSet) {
            returnStatisticsVector.emplace_back(statisticHash, stat);
        }
    }
    return returnStatisticsVector;
}

SortedVectorStatisticStore::~SortedVectorStatisticStore() = default;
}// namespace NES::Statistic
