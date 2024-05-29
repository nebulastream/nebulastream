//
// Created by Noah Stöcker on 25.05.24.
//

#ifndef HYBRIDSTATISTICSTORE_HPP
#define HYBRIDSTATISTICSTORE_HPP

// Using an unordered_map for finding the correct query
// Using a red-black tree (std::map) (=binary search tree) for storage of statistics. Efficient range queries. Ordered.
// Combines efficiency of hash tables with trees. Balanced approach

#include "AbstractStatisticStore.hpp"
#include <Statistics/Statistic.hpp>
#include <Statistics/StatisticKey.hpp>
#include <map>
#include <unordered_map>

namespace NES::Statistic {

class HybridStatisticStore : public AbstractStatisticStore {
private:
    using TimestampedStatistic = std::pair<Windowing::TimeMeasure, StatisticPtr>;
    std::unordered_map<StatisticHash, std::map<Windowing::TimeMeasure, StatisticPtr>> table;
public:
    std::vector<StatisticPtr> getStatistics(const StatisticHash& statisticHash, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs) override;

    std::optional<StatisticPtr> getSingleStatistic(const StatisticHash& statisticHash,
                                               const Windowing::TimeMeasure& startTs,
                                               const Windowing::TimeMeasure& endTs) override;

    std::vector<HashStatisticPair> getAllStatistics() override;

    bool insertStatistic(const StatisticHash& statisticHash, StatisticPtr statistic) override;

    bool deleteStatistics(const StatisticHash& statisticHash, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs) override;

    ~HybridStatisticStore() override = default;

};
}



#endif //HYBRIDSTATISTICSTORE_HPP
