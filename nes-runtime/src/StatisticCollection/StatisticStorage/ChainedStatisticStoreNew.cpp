#include <StatisticCollection/StatisticStorage/ChainedStatisticStoreNew.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Statistic {

StatisticStorePtr ChainedStatisticStoreNew::create() { return std::make_shared<ChainedStatisticStoreNew>(); }

std::vector<StatisticPtr> ChainedStatisticStoreNew::getStatistics(const StatisticHash& statisticHash,
                                                                  const Windowing::TimeMeasure& startTs,
                                                                  const Windowing::TimeMeasure& endTs) {
    std::vector<StatisticPtr> returnStatisticsVector;
    auto& statisticList = chainedStatistics[statisticHash];

    for (const auto& statistic : statisticList) {
        if (startTs <= statistic->getStartTs() && statistic->getEndTs() <= endTs) {
            returnStatisticsVector.push_back(statistic);
        }
    }

    return returnStatisticsVector;
}

std::optional<StatisticPtr> ChainedStatisticStoreNew::getSingleStatistic(const StatisticHash& statisticHash,
                                                                         const Windowing::TimeMeasure& startTs,
                                                                         const Windowing::TimeMeasure& endTs) {
    auto& statisticList = chainedStatistics[statisticHash];

    for (const auto& statistic : statisticList) {
        if (startTs == statistic->getStartTs() && endTs == statistic->getEndTs()) {
            return statistic;
        }
    }

    return {};
}

bool ChainedStatisticStoreNew::insertStatistic(const StatisticHash& statisticHash, StatisticPtr statistic) {
    auto& statisticList = chainedStatistics[statisticHash];

    for (const auto& stat : statisticList) {
        if (statistic->getStartTs().equals(stat->getStartTs()) && statistic->getEndTs().equals(stat->getEndTs())) {
            return false;
        }
    }

    statisticList.push_back(statistic);
    return true;
}

bool ChainedStatisticStoreNew::deleteStatistics(const StatisticHash& statisticHash,
                                                const Windowing::TimeMeasure& startTs,
                                                const Windowing::TimeMeasure& endTs) {
    auto& statisticList = chainedStatistics[statisticHash];
    auto it = statisticList.begin();
    bool isDeleted = false;

    while (it != statisticList.end()) {
        if (startTs <= (*it)->getStartTs() && (*it)->getEndTs() <= endTs) {
            it = statisticList.erase(it);
            isDeleted = true;
        } else {
            ++it;
        }
    }

    return isDeleted;
}

std::vector<HashStatisticPair> ChainedStatisticStoreNew::getAllStatistics() {
    std::vector<HashStatisticPair> returnStatisticsVector;

    for (const auto& [statisticHash, statisticList] : chainedStatistics) {
        for (const auto& statistic : statisticList) {
            returnStatisticsVector.emplace_back(statisticHash, statistic);
        }
    }

    return returnStatisticsVector;
}

}// namespace NES::Statistic
