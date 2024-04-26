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

#ifndef NES_STATISTICS_INCLUDE_STATISTICCOLLECTION_STATISTICREGISTRY_STATISTICREGISTRY_HPP_
#define NES_STATISTICS_INCLUDE_STATISTICCOLLECTION_STATISTICREGISTRY_STATISTICREGISTRY_HPP_

#include <Operators/LogicalOperators/StatisticCollection/TriggerCondition/TriggerCondition.hpp>
#include <Statistics/StatisticKey.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticInfo.hpp>
#include <folly/Synchronized.h>
#include <functional>
#include <map>
#include <optional>

namespace NES::Statistic {

class StatisticRegistry;
using StatisticRegistryPtr = std::shared_ptr<StatisticRegistry>;

/**
 * @brief This registry stores StatisticInfo for each StatisticKey. For now, we do not support that a same statistic key
 * can have different triggerCondition and callbacks, see issue #4776 and issue #4778
 */
class StatisticRegistry {
  public:

    static StatisticRegistryPtr create();

    /**
     * @brief Gets a StatisticInfo for a StatisticKey
     * @param statisticHash
     * @return StatisticInfoWLock
     */
    StatisticInfoWLock getStatisticInfo(const StatisticHash statisticHash);

    /**
     * @brief Gets the queryId for a StatisticKey
     * @param statisticHash
     * @return QueryId
     */
    QueryId getQueryId(const StatisticHash statisticHash) const;

    /**
     * @brief Gets a StatisticInfo for a StatisticKey and checks if the StatisticInfo has the same granularity
     * @param statisticHash
     * @param granularity: Granularity, i.e., 5 Seconds, of the window the statistic is built on
     * @return std::optional<StatisticInfoWLock>
     */
    std::optional<StatisticInfoWLock> getStatisticInfoWithGranularity(const StatisticHash statisticHash,
                                                                      const Windowing::TimeMeasure& granularity);

    /**
     * @brief Inserts a StatisticKey and the StatisticInfo into this StatisticRegistry
     * @param statisticHash
     * @param statisticInfo
     */
    void insert(const StatisticHash statisticHash, const StatisticInfo statisticInfo);

    /**
     * @brief Checks if this registry contains the statisticHash already
     * @param statisticHash
     * @return True, if this registry contains the statisticHash already. False, otherwise.
     */
    bool contains(const StatisticHash statisticHash);

    /**
     * @brief Query belonging to this StatisticKey is stopped. Therefore, we set the queryId to INVALID_QUERY_ID.
     * @param statisticHash
     */
    void queryStopped(const StatisticHash statisticHash);

    /**
     * @brief Deletes/clears all underlying stored data
     */
    void clear();

    /**
     * @brief Checks if a query is running according to this registry
     * @param statisticHash
     * @return True, if queryId != INVALID_QUERY_ID
     */
    bool isRunning(const StatisticHash statisticHash) const;

  private:
    folly::Synchronized<std::unordered_map<StatisticHash, folly::Synchronized<StatisticInfo>>>
        keyToStatisticInfo;
};

}// namespace NES::Statistic

#endif // NES_STATISTICS_INCLUDE_STATISTICCOLLECTION_STATISTICREGISTRY_STATISTICREGISTRY_HPP_
