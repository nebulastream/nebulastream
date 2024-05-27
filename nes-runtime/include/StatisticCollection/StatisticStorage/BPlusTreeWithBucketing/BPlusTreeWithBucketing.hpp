//
// Created by veronika on 20.05.24.
//

#ifndef NES_BPLUSTREEWITHBUCKETING_HPP
#define NES_BPLUSTREEWITHBUCKETING_HPP



#include "StatisticCollection/StatisticStorage/AbstractStatisticStore.hpp"

#include "Node.hpp"
#include <folly/Synchronized.h>
#include <unordered_map>

namespace NES::Statistic {
class BPlusTreeWithBucketing : public AbstractStatisticStore {
  protected:

    uint64_t Capacity{};
    Node root = Node(0, true);
    uint64_t bucketsRangeSize;
    std::vector<Node> allParents = {};

  public:

    static StatisticStorePtr create(uint64_t& Capacity, uint64_t& bucketsRangeSize);
    /**
     * @brief Gets all statistics belonging to the statisticHash in the period of [startTs, endTs]
     * @param statisticHash
     * @param startTs
     * @param endTs
     * @return Vector of StatisticPtr
     */

    std::vector<StatisticPtr> getStatistics(const StatisticHash& statisticHash,
                                            const Windowing::TimeMeasure& startTs,
                                            const Windowing::TimeMeasure& endTs) override;
    /**
     * @brief Gets a single statistic belonging to the statisticHash that has exactly the startTs and endTs
     * @param statisticHash
     * @param startTs
     * @param endTs
     * @return optional<StatisticPtr>
     */
    std::optional<StatisticPtr> getSingleStatistic(const StatisticHash& statisticHash,
                                                   const Windowing::TimeMeasure& startTs,
                                                   const Windowing::TimeMeasure& endTs) override;
    /**
     * @brief Returns all statistics currently in this store
     * @return Vector of HashStatisticPair
     */
    std::vector<HashStatisticPair> getAllStatistics() override;

    /**
     * @brief Inserts statistic with the statisticHash into a StatisticStore.
     * @param statisticHash
     * @param statistic
     * @return Success
     */

    bool insertStatistic(const StatisticHash& statisticHash, StatisticPtr statistic) override;

    /**
     * @brief Deletes all statistics belonging to the statisticHash in the period of [startTs, endTs]
     * @param statisticHash
     * @param startTs
     * @param endTs
     * @return Success
     */
    bool deleteStatistics(const StatisticHash& statisticHash,
                          const Windowing::TimeMeasure& startTs,
                          const Windowing::TimeMeasure& endTs) override;


    //other help methods

    Node getParent(Node& node);

    Node searchNode(StatisticHash& key, Node& node);

    bool findOrCreateBucket(Node& leaf, const StatisticPtr& statistic, const StatisticHash& statisticHash) const;
    virtual ~BPlusTreeWithBucketing();

};


}
#endif//NES_BPLUSTREEWITHBUCKETING_HPP