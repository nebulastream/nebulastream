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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICCACHE_ABSTRACTSTATISTICCACHE_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICCACHE_ABSTRACTSTATISTICCACHE_HPP_

#include <Statistics/StatisticValue.hpp>
#include <Measures/TimeMeasure.hpp>
#include <Statistics/StatisticKey.hpp>
#include <memory>

namespace NES::Statistic {

class AbstractStatisticCache;
using StatisticCachePtr = std::shared_ptr<AbstractStatisticCache>;

/**
 * @brief Abstract class that defines an interface for all statistic caches
 */
class AbstractStatisticCache {
  public:
    /**
     * @brief Inserts a statistic into the cache. If a statistic exists for the same timestamp then, it will NOT be overwritten and discarded
     * @param statisticHash
     * @param statisticValue
     * @return True, if the statistic was inserted successfully, false otherwise
     */
    virtual bool insertStatistic(const StatisticHash& statisticHash, const StatisticValue<>& statisticValue) = 0;

    /**
     * @brief Gets the statistic for the given hash and interval [startTs, endTs]
     * @param statisticHash
     * @param startTs
     * @param endTs
     * @return Vector of StatisticValuePtr
     */
    virtual std::vector<StatisticValuePtr> getStatistic(const StatisticHash& statisticHash,
                                                        const Windowing::TimeMeasure& startTs,
                                                        const Windowing::TimeMeasure& endTs) = 0;

    /**
     * @brief Deletes the statistics for the given hash and interval [startTs, endTs]
     * @param statisticHash
     * @param startTs
     * @param endTs
     * @return True, if at least one statistic was deleted
     */
    virtual bool deleteStatistics(const StatisticHash& statisticHash,
                                  const Windowing::TimeMeasure& startTs,
                                  const Windowing::TimeMeasure& endTs) = 0;

    virtual ~AbstractStatisticCache();
};

}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICCACHE_ABSTRACTSTATISTICCACHE_HPP_
