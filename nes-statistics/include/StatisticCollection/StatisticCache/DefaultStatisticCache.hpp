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

#ifndef NES_STATISTICS_INCLUDE_STATISTICCOLLECTION_STATISTICCACHE_DEFAULTSTATISTICCACHE_HPP_
#define NES_STATISTICS_INCLUDE_STATISTICCOLLECTION_STATISTICCACHE_DEFAULTSTATISTICCACHE_HPP_

#include <Statistics/StatisticValue.hpp>
#include <StatisticCollection/StatisticCache/AbstractStatisticCache.hpp>
#include <Statistics/StatisticKey.hpp>
#include <folly/Synchronized.h>
#include <unordered_map>

namespace NES::Statistic {

/**
 * @brief Default implementation of the statistic cache. It does not overwrite any statistic and just stores any statistic
 */
class DefaultStatisticCache : public AbstractStatisticCache {
  public:
    /**
     * @brief Method to create a new instance of the DefaultStatisticCache
     * @return StatisticCachePtr
     */
    static StatisticCachePtr create();

    /**
     * @brief Inserts a statistic into the cache if it does not exist yet
     * @param statisticHash
     * @param statisticValue
     * @return Success of the operation
     */
    bool insertStatistic(const StatisticHash& statisticHash, const StatisticValue<>& statisticValue) override;

    /**
     * @brief Retrieves the statistic from the cache
     * @param statisticHash
     * @param startTs
     * @param endTs
     * @return Value of StatisticValuePtr
     */
    std::vector<StatisticValuePtr> getStatistic(const StatisticHash& statisticHash,
                                                const Windowing::TimeMeasure& startTs,
                                                const Windowing::TimeMeasure& endTs) override;

    /**
     * @brief Deletes the statistics from the cache
     * @param statisticHash
     * @param startTs
     * @param endTs
     * @return Success of the operation
     */
    bool deleteStatistics(const StatisticHash& statisticHash,
                          const Windowing::TimeMeasure& startTs,
                          const Windowing::TimeMeasure& endTs) override;

  private:
    folly::Synchronized<std::unordered_map<StatisticHash, std::vector<StatisticValuePtr>>> keyToStatistics;
};
}// namespace NES::Statistic
#endif // NES_STATISTICS_INCLUDE_STATISTICCOLLECTION_STATISTICCACHE_DEFAULTSTATISTICCACHE_HPP_
