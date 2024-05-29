//
// Created by Noah Stöcker on 20.05.24.
//

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICSTORAGE_CHAINEDSTATISTICSTORE_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICSTORAGE_CHAINEDSTATISTICSTORE_HPP_

#include <StatisticCollection/StatisticStorage/AbstractStatisticStore.hpp>

namespace NES::Statistic {

class ChainedStatisticStore : public AbstractStatisticStore {
    public:

    static StatisticStorePtr create();

    std::vector <StatisticPtr> getStatistics(const StatisticHash &statisticHash,
                                             const Windowing::TimeMeasure &startTs,
                                             const Windowing::TimeMeasure &endTs) override;

    std::vector<HashStatisticPair> getAllStatistics() override;

  std::optional<StatisticPtr> getSingleStatistic(const StatisticHash& statisticHash,
                                                 const Windowing::TimeMeasure& startTs,
                                                 const Windowing::TimeMeasure& endTs) override;

    bool insertStatistic(const StatisticHash& statisticHash, StatisticPtr statistic) override;

    bool deleteStatistics(const StatisticHash& statisticHash,
                          const Windowing::TimeMeasure& startTs,
                          const Windowing::TimeMeasure& endTs) override;

     ~ChainedStatisticStore() override = default;

};

}


#endif
