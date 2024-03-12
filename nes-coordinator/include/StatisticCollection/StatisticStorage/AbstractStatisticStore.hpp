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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICSTORAGE_ABSTRACTSTATISTICSTORE_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICSTORAGE_ABSTRACTSTATISTICSTORE_HPP_

#include <Operators/LogicalOperators/StatisticCollection/Statistics/Statistic.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticKey.hpp>

namespace NES::Statistic {

class AbstractStatisticStore;
using AbstractStatisticStorePtr = std::shared_ptr<AbstractStatisticStore>;

/**
 * @brief An interface for any statistic store
 */
class AbstractStatisticStore {
  public:
    /**
     * @brief Gets all statistics belonging to the statisticHash in the period of [startTs, endTs]
     * @param statisticHash
     * @param startTs
     * @param endTs
     * @return Vector of StatisticPtr
     */
    virtual std::vector<StatisticPtr> getStatistics(const StatisticHash& statisticHash,
                                                    const Windowing::TimeMeasure& startTs,
                                                    const Windowing::TimeMeasure& endTs) = 0;

    /**
     * @brief Inserts statistic with the statisticHash into a StatisticStore.
     * @param statisticHash
     * @param statistic
     * @return Success
     */
    virtual bool insertStatistic(const StatisticHash& statisticHash, StatisticPtr statistic) = 0;

    /**
     * @brief Deletes all statistics belonging to the statisticHash in the period of [startTs, endTs]
     * @param statisticHash
     * @param startTs
     * @param endTs
     * @return Success
     */
    virtual bool deleteStatistics(const StatisticHash& statisticHash,
                                  const Windowing::TimeMeasure& startTs,
                                  const Windowing::TimeMeasure& endTs) = 0;

    /**
     * @brief Virtual destructor
     */
    virtual ~AbstractStatisticStore() = default;
};
}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICSTORAGE_ABSTRACTSTATISTICSTORE_HPP_
