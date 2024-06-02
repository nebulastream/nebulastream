#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICSTORAGE_SORTEDSTATISTICSTORE_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICSTORAGE_SORTEDSTATISTICSTORE_HPP_

#include <StatisticCollection/StatisticStorage/AbstractStatisticStore.hpp>
#include <folly/Synchronized.h>
#include <unordered_map>
#include <vector>
#include <algorithm>
namespace NES::Statistic {

/**
 * @brief This is a thread-safe StatisticStore that simply stores all data in an unordered_map that is made thread-safe
 * by using folly::Synchronized
 */
class SortedStatisticStore : public AbstractStatisticStore {
  public:
    /**
     * @brief Creates a SortedStatisticStore
     * @return StatisticStorePtr
     */
    static StatisticStorePtr create();

    std::vector<StatisticPtr> getStatistics(const StatisticHash& statisticHash,
                                            const Windowing::TimeMeasure& startTs,
                                            const Windowing::TimeMeasure& endTs) override;

    std::optional<StatisticPtr> getSingleStatistic(const StatisticHash& statisticHash,
                                                   const Windowing::TimeMeasure& startTs,
                                                   const Windowing::TimeMeasure& endTs) override;
                                                   
    std::vector<HashStatisticPair> getAllStatistics() override;

    /**
     * @brief Implements the insert of the interface. If a statistic exists with the same startTs and endTs, we do not
     * allow the insertion.
     * @param statisticKey
     * @param statistic
     * @return True, if the statistic was inserted into this storage.
     */
    bool insertStatistic(const StatisticHash& statisticHash, StatisticPtr statistic) override;

    /**
     * @brief Implements the delete of the interface for all statistics in the period [startTs, endTs]
     * @param statisticKey
     * @param startTs
     * @param endTs
     * @return True, if at least one statistic was deleted
     */
    bool deleteStatistics(const StatisticHash& statisticHash,
                          const Windowing::TimeMeasure& startTs,
                          const Windowing::TimeMeasure& endTs) override;

    /**
     * @brief Virtual destructor
     */
    virtual ~SortedStatisticStore();

  private:
    //SortedVectorStatisticStore
    struct StatisticCompare {
        bool operator()(const StatisticPtr& lhs, const StatisticPtr& rhs) const {
            return lhs->getStartTs() < rhs->getStartTs();
        }
    }; //SortedVectorStatisticStore

    folly::Synchronized<std::unordered_map<StatisticHash, std::vector<StatisticPtr>>> keyToStatistics;
};

}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICSTORAGE_SORTEDSTATISTICSTORE_HPP_