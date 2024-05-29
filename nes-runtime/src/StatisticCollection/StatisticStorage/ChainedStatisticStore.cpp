#include <StatisticCollection/StatisticStorage/ChainedStatisticStore.hpp>
#include <Util/Logger/Logger.hpp>
#include <StatisticCollection/StatisticStorage/AbstractStatisticStore.hpp>
#include <cstdint>

namespace NES::Statistic {

StatisticStorePtr ChainedStatisticStore::create() { return std::make_shared<ChainedStatisticStore>(); }

struct TimedStatistic {
    uint64_t timestamp;
    StatisticPtr statistic;

    TimedStatistic(uint64_t  timestamp, StatisticPtr statistic)
        : timestamp(std::move(timestamp)), statistic(std::move(statistic)) {}
};

std::unordered_map<StatisticHash, std::vector<TimedStatistic>> table;

/*std::vector<StatisticPtr> ChainedStatisticStore::getStatistics(const StatisticHash& statisticHash,
                                                               const Windowing::TimeMeasure& startTs,
                                                               const Windowing::TimeMeasure& endTs) {
    std::vector<StatisticPtr> returnStatisticsVector;
    auto it = table.find(statisticHash);

    if (it != table.end()) {
        auto& timedStatistics = it->second;

        auto getCondition = [startTs, endTs](const TimedStatistic& timedStatistic) {
            return startTs <= timedStatistic.statistic->getStartTs() && timedStatistic.statistic->getEndTs() <= endTs;
        };

        for (const auto& timedStatistic : timedStatistics) {
            if (getCondition(timedStatistic)) {
                returnStatisticsVector.push_back(timedStatistic.statistic);
            }
        }
    }

    return returnStatisticsVector;
}*/

std::vector<StatisticPtr> ChainedStatisticStore::getStatistics(const StatisticHash& statisticHash,
                                                               const Windowing::TimeMeasure& startTs,
                                                               const Windowing::TimeMeasure& endTs) {

    NES_WARNING("ChainedStatisticStore::getStatistics REACHED!")
    std::vector<StatisticPtr> returnStatisticsVector;
    auto it = table.find(statisticHash);

    if (it != table.end()) {
        auto& timedStatistics = it->second;

        for (const auto& timedStatistic : timedStatistics) {
            if (startTs <= timedStatistic.statistic->getStartTs() && timedStatistic.statistic->getEndTs() <= endTs) {
                returnStatisticsVector.push_back(timedStatistic.statistic);
            }
        }
    }

    return returnStatisticsVector;
}

std::optional<StatisticPtr> ChainedStatisticStore::getSingleStatistic(const StatisticHash& statisticHash,
                                                                      const Windowing::TimeMeasure& startTs,
                                                                      const Windowing::TimeMeasure& endTs) {
    auto it = table.find(statisticHash);

    if (it != table.end()) {
        auto& timedStatistics = it->second;

        for (const auto& timedStatistic : timedStatistics) {
            if (startTs == timedStatistic.statistic->getStartTs() && endTs == timedStatistic.statistic->getEndTs()) {
                return timedStatistic.statistic;
            }
        }
    }

    return {};
}


bool ChainedStatisticStore::deleteStatistics(const StatisticHash& statisticHash,
                                             const Windowing::TimeMeasure& startTs,
                                             const Windowing::TimeMeasure& endTs) {
    auto it = table.find(statisticHash);

    if (it != table.end()) {
        auto& timedStatistics = it->second;

        auto deleteCondition = [startTs, endTs](const TimedStatistic& timedStatistic) {
            return startTs <= timedStatistic.statistic->getStartTs() && timedStatistic.statistic->getEndTs() <= endTs;
        };

        auto removeBeginIt = std::remove_if(timedStatistics.begin(), timedStatistics.end(), deleteCondition);
        const bool foundAnyStatistic = removeBeginIt != timedStatistics.end();
        timedStatistics.erase(removeBeginIt, timedStatistics.end());
        return foundAnyStatistic;
    }

    return false;
}

bool ChainedStatisticStore::insertStatistic(const StatisticHash& statisticHash, StatisticPtr statistic) {
    uint64_t currentTimestamp = statistic->getStartTs().getTime();
    TimedStatistic timedStatistic(currentTimestamp, statistic);
    table[statisticHash].push_back(timedStatistic);
    return true;
}

std::vector<HashStatisticPair> ChainedStatisticStore::getAllStatistics() {
    std::vector<HashStatisticPair> returnStatisticsVector;

    for (const auto& [statisticHash, timedStatistics] : table) {
        for (const auto& timedStatistic : timedStatistics) {
            returnStatisticsVector.emplace_back(statisticHash, timedStatistic.statistic);
        }
    }

    return returnStatisticsVector;
}

}