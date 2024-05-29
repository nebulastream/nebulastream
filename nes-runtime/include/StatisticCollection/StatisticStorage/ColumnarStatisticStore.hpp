//
// Created by Noah Stöcker on 25.05.24.
//

#ifndef COLUMNARSTATISTICSTORAGE_HPP
#define COLUMNARSTATISTICSTORAGE_HPP

// timestamps = hash table of statistic hash to vector of timestamps
// statistics = hash table of statistic hash to vector of statistics

// optimized for read-heavy workloads (especially getAllStatistics)
#include "AbstractStatisticStore.hpp"
#include <Statistics/Statistic.hpp>
#include <Statistics/StatisticKey.hpp>
#include <unordered_map>

namespace NES::Statistic {

class ColumnarStatisticStore : public AbstractStatisticStore {
private:
    std::unordered_map<StatisticHash, std::vector<uint64_t>> timestamps;
    std::unordered_map<StatisticHash, std::vector<StatisticPtr>> statistics;
public:
    std::vector<StatisticPtr> getStatistics(const StatisticHash& statisticHash, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs) override;

    std::vector<HashStatisticPair> getAllStatistics() override;

    bool insertStatistic(const StatisticHash& statisticHash, StatisticPtr statistic) override;

    bool deleteStatistics(const StatisticHash& statisticHash, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs) override;

    ~ColumnarStatisticStore() override = default;
};
}





#endif //COLUMNARSTATISTICSTORAGE_HPP
