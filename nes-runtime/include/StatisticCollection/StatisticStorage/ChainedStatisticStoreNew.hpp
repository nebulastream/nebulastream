#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICSTORAGE_CHAINEDSTATISTICSTORENEW_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICSTORAGE_CHAINEDSTATISTICSTORENEW_HPP_

#include "StatisticCollection/StatisticStorage/AbstractStatisticStore.hpp"
#include <list>
#include <unordered_map>

namespace NES::Statistic {

class ChainedStatisticStoreNew : public AbstractStatisticStore {
public:
    static StatisticStorePtr create();

    std::vector<StatisticPtr> getStatistics(const StatisticHash& statisticHash,
                                            const Windowing::TimeMeasure& startTs,
                                            const Windowing::TimeMeasure& endTs) override;

  std::optional<StatisticPtr> getSingleStatistic(const StatisticHash& statisticHash,
                                               const Windowing::TimeMeasure& startTs,
                                               const Windowing::TimeMeasure& endTs) override;

    bool insertStatistic(const StatisticHash& statisticHash, StatisticPtr statistic) override;

    bool deleteStatistics(const StatisticHash& statisticHash,
                          const Windowing::TimeMeasure& startTs,
                          const Windowing::TimeMeasure& endTs) override;

    std::vector<HashStatisticPair> getAllStatistics() override;

    ~ChainedStatisticStoreNew() override = default;

private:
    // Lists are better for deletion and insertion (especially in the middle of the list)
    // Vectors are better for random access and space efficiency
    std::unordered_map<StatisticHash, std::list<StatisticPtr>> chainedStatistics;
};

}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICSTORAGE_CHAINEDSTATISTICSTORENEW_HPP_