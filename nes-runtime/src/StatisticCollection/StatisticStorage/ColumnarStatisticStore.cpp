//
// Created by Noah Stöcker on 25.05.24.
//

#include "StatisticCollection/StatisticStorage/ColumnarStatisticStore.hpp"
#include <Util/Logger/Logger.hpp>

namespace NES::Statistic {

std::vector<StatisticPtr> ColumnarStatisticStore::getStatistics(const StatisticHash& statisticHash,
                                                                const Windowing::TimeMeasure& startTs,
                                                                const Windowing::TimeMeasure& endTs) {
    std::vector<StatisticPtr> returnStatisticsVector;
    auto itTimestamps = timestamps.find(statisticHash);
    auto itStatistics = statistics.find(statisticHash);

    if (itTimestamps != timestamps.end() && itStatistics != statistics.end()) {
        const auto& tsVec = itTimestamps->second;
        const auto& statVec = itStatistics->second;

        for (size_t i = 0; i < tsVec.size(); ++i) {
            if (startTs.getTime() <= tsVec[i] && tsVec[i] <= endTs.getTime()) {
                returnStatisticsVector.emplace_back(statVec[i]);
            }
        }
    }

    return returnStatisticsVector;
}

std::vector<HashStatisticPair> ColumnarStatisticStore::getAllStatistics() {
    std::vector<HashStatisticPair> result;
    for (const auto& entry : statistics) {
        const auto& hash = entry.first;
        const auto& statVec = entry.second;
        for (const auto& statistic : statVec) {
            result.emplace_back(hash, statistic);
        }
    }
    return result;
}

bool ColumnarStatisticStore::insertStatistic(const StatisticHash& statisticHash, StatisticPtr statistic) {
    Windowing::TimeMeasure currentTimestamp = statistic->getStartTs();
    timestamps[statisticHash].emplace_back(currentTimestamp.getTime());
    statistics[statisticHash].emplace_back(statistic);
    return true;
}

bool ColumnarStatisticStore::deleteStatistics(const StatisticHash& statisticHash,
                                              const Windowing::TimeMeasure& startTs,
                                              const Windowing::TimeMeasure& endTs) {
    auto itTimestamps = timestamps.find(statisticHash);
    auto itStatistics = statistics.find(statisticHash);

    if (itTimestamps != timestamps.end() && itStatistics != statistics.end()) {
        auto& tsVec = itTimestamps->second;
        auto& statVec = itStatistics->second;

        auto itTs = tsVec.begin();
        auto itStat = statVec.begin();
        while (itTs != tsVec.end() && itStat != statVec.end()) {
            if (startTs.getTime() <= *itTs && *itTs <= endTs.getTime()) {
                itTs = tsVec.erase(itTs);
                itStat = statVec.erase(itStat);
            } else {
                ++itTs;
                ++itStat;
            }
        }

        if (tsVec.empty()) {
            timestamps.erase(itTimestamps);
            statistics.erase(itStatistics);
        }
        return true;
    }

    return false;
}


}