//
// Created by Noah Stöcker on 25.05.24.
//
#include <StatisticCollection/StatisticStorage/HybridStatisticStore.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Statistic {

std::vector<StatisticPtr> HybridStatisticStore::getStatistics(const StatisticHash& statisticHash,
                                                              const Windowing::TimeMeasure& startTs,
                                                                const Windowing::TimeMeasure& endTs) {
    std::vector<StatisticPtr> returnStatisticsVector;
    auto it = table.find(statisticHash);
    if (it != table.end()) {
        auto& timestampedMap = it->second;
        auto startIt = timestampedMap.lower_bound(startTs);
        auto endIt = timestampedMap.upper_bound(endTs);

        for (auto iter = startIt; iter != endIt; ++iter) {
            returnStatisticsVector.push_back(iter->second);
        }
    }
    return returnStatisticsVector;
}

std::optional<StatisticPtr> HybridStatisticStore::getSingleStatistic(const StatisticHash& statisticHash,
                                                                     const Windowing::TimeMeasure& startTs,
                                                                     const Windowing::TimeMeasure& endTs) {
    auto it = table.find(statisticHash);
    if (it != table.end()) {
        auto& timestampedMap = it->second;
        auto statisticIt = timestampedMap.find(startTs);

        if (statisticIt != timestampedMap.end() && statisticIt->second->getEndTs() == endTs) {
            return statisticIt->second;
        }
    }

    return {};
}

std::vector<HashStatisticPair> HybridStatisticStore::getAllStatistics() {
    std::vector<HashStatisticPair> result;
    for (const auto& entry : table) {
        for (const auto& timestampedStatistic : entry.second) {
            result.emplace_back(entry.first, timestampedStatistic.second);
        }
    }
    return result;
}

bool HybridStatisticStore::insertStatistic(const StatisticHash& statisticHash, StatisticPtr statistic) {
    Windowing::TimeMeasure currentTimestamp = statistic->getStartTs();
    table[statisticHash][currentTimestamp] = statistic;
    return true;
}

bool HybridStatisticStore::deleteStatistics(const StatisticHash& statisticHash,
                                            const Windowing::TimeMeasure& startTs,
                                            const Windowing::TimeMeasure& endTs) {
    auto it = table.find(statisticHash);
    if (it != table.end()) {
        auto& timestampedMap = it->second;
        auto startIt = timestampedMap.lower_bound(startTs);
        auto endIt = timestampedMap.upper_bound(endTs);

        if (startIt != endIt) {
            timestampedMap.erase(startIt, endIt);
        }

        if (timestampedMap.empty()) {
            table.erase(it);
        }
        return true;
    }
    return false;
}
}// namespace NES::Statistic