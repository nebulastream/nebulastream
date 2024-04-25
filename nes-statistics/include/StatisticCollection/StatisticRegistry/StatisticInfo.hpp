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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICREGISTRY_STATISTICINFO_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICREGISTRY_STATISTICINFO_HPP_

#include <Identifiers/Identifiers.hpp>
#include <Measures/TimeMeasure.hpp>
#include <Types/WindowType.hpp>
#include <StatisticCollection/Characteristic/Characteristic.hpp>
#include <Operators/LogicalOperators/StatisticCollection/TriggerCondition/TriggerCondition.hpp>
#include <folly/Synchronized.h>
#include <functional>

namespace NES::Statistic {

/**
 * @brief Combines together a triggerCondition, callback function and a queryId for a specific StatisticKey
 */
class StatisticInfo {
  public:
    StatisticInfo() = default;

    /**
     * @brief Constructor for a StatisticInfo
     * @param window: Over what window do we built the statistic, e.g., Tumbling, Sliding, Threshold, ...
     * @param triggerCondition: Condition that is checked for each newly created window of a given statistic.
     * @param callBack: Function that is called, if triggerCondition returns true
     * @param queryId: Id of the StatisticQuery
     * @param metric: Metric for this statistic
     */
    StatisticInfo(const Windowing::WindowTypePtr window,
                  const TriggerConditionPtr triggerCondition,
                  const std::function<void(CharacteristicPtr)> callBack,
                  const QueryId& queryId,
                  const MetricPtr metric);

    /**
     * @brief Gets the TriggerCondition
     * @return TriggerConditionPtr
     */
    [[maybe_unused]] TriggerConditionPtr getTriggerCondition() const;

    /**
     * @brief Gets the callBack function
     * @return const std::function<void(Characteristic)>&
     */
    const std::function<void(CharacteristicPtr)>& getCallBack() const;

    /**
     * @brief Gets the queryId
     * @return QueryId
     */
    QueryId getQueryId() const;

    /**
     * @brief The query with the id is not running anymore, therefore, we have to set the queryId to INVALID_QUERY_ID
     */
    void stoppedQuery();

    /**
     * @brief Compares the underlying queryId to INVALID_QUERY_ID. If they are equal, the query is not running
     * @return True, if query is running, false otherwise
     */
    bool isRunning() const;

    /**
     * @brief Sets the queryId
     * @param queryId
     */
    void setQueryId(const QueryId queryId);

    /**
     * @brief Gets the Window
     * @return WindowTypePtr
     */
    Windowing::WindowTypePtr getWindow() const;

    /**
     * @brief Gets the Metric
     * @return MetricPtr
     */
    MetricPtr getMetric() const;

    /**
     * @brief Checks for equality
     * @param rhs
     * @return True, if equal otherwise false
     */
    bool operator==(const StatisticInfo& rhs) const;

    /**
     * @brief Checks for equality
     * @param rhs
     * @return True, if NOT equal otherwise false
     */
    bool operator!=(const StatisticInfo& rhs) const;

    /**
     * @brief Creates a string representation
     * @return std::string
     */
    [[nodiscard]] std::string toString() const;

  private:
    Windowing::WindowTypePtr window;
    TriggerConditionPtr triggerCondition;
    std::function<void(CharacteristicPtr)> callBack;
    QueryId queryId = INVALID_QUERY_ID;
    MetricPtr metric;
};
using StatisticInfoWLock = std::shared_ptr<folly::Synchronized<StatisticInfo>::WLockedPtr>;

}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICREGISTRY_STATISTICINFO_HPP_
