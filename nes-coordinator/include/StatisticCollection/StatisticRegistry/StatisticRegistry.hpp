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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICREGISTRY_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICREGISTRY_HPP_

#include <Operators/LogicalOperators/StatisticCollection/TriggerCondition/TriggerCondition.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticInfo.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticKey.hpp>
#include <folly/Synchronized.h>
#include <functional>
#include <map>

namespace NES::Statistic {

/**
 * @brief This registry stores StatisticInfo for each StatisticKey
 */
class StatisticRegistry {
  public:
    /**
     * @brief Gets a StatisticInfo for a StatisticKey
     * @param statisticKey
     * @return StatisticInfoWLock
     */
    StatisticInfoWLock getStatisticInfo(const StatisticKey statisticKey);

    /**
     * @brief Gets a StatisticInfo for a StatisticKey
     * @param statisticKey
     * @return StatisticInfoWLock
     */
    std::vector<StatisticInfoWLock> getStatisticInfo(const QueryId queryId);

    /**
     * @brief Inserts a StatisticKey and the StatisticInfo into this StatisticRegistry
     * @param statisticKey
     * @param statisticInfo
     */
    void insert(const StatisticKey statisticKey, const StatisticInfo statisticInfo);

    /**
     * @brief Query belonging to this StatisticKey is stopped. Therefore, we set the queryId to INVALID_QUERY_ID.
     * @param statisticKey
     */
    void queryStopped(const StatisticKey statisticKey);

    /**
     * @brief Deletes/clears all underlying stored data
     */
    void clear();

    /**
     * @brief Checks if a query is running according to this registry
     * @param statisticKey
     * @return True, if queryId != INVALID_QUERY_ID
     */
    bool isRunning(const StatisticKey statisticKey) const;

  private:
    folly::Synchronized<std::unordered_map<StatisticKey, folly::Synchronized<StatisticInfo>, StatisticKeyHash>>
        keyToStatisticInfo;
};

}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICREGISTRY_HPP_
