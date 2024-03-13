/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICSTORAGE_DEFAULTSTATISTICSTORE_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICSTORAGE_DEFAULTSTATISTICSTORE_HPP_

#include <StatisticCollection/StatisticStorage/AbstractStatisticStore.hpp>
#include <folly/Synchronized.h>
#include <unordered_map>
namespace NES::Statistic {

/**
 * @brief This is a thread-safe StatisticStore that simply stores all data in an unordered_map that is made thread-safe
 * by using folly::Synchronized
 */
class DefaultStatisticStore : public AbstractStatisticStore {
  public:
    /**
     * @brief Creates a DefaultStatisticStore
     * @return AbstractStatisticStorePtr
     */
    static AbstractStatisticStorePtr create();

    std::vector<StatisticPtr> getStatistics(const StatisticHash& statisticHash,
                                            const Windowing::TimeMeasure& startTs,
                                            const Windowing::TimeMeasure& endTs) override;

    std::vector<StatisticPtr> getAllStatistics() override;

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
    virtual ~DefaultStatisticStore();

  private:
    folly::Synchronized<std::unordered_map<StatisticHash, std::vector<StatisticPtr>>> keyToStatistics;
};

}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICSTORAGE_DEFAULTSTATISTICSTORE_HPP_
