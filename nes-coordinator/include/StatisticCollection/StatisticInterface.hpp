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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTIC_STATISTICINTERFACE_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTIC_STATISTICINTERFACE_HPP_

#include <Operators/LogicalOperators/Windows/WindowingForwardRefs.hpp>
#include <StatisticCollection/Characteristic/Characteristic.hpp>
#include <StatisticCollection/Statistic/StatisticValue.hpp>
#include <StatisticCollection/SendingPolicy/SendingPolicy.hpp>
#include <StatisticCollection/TriggerCondition/TriggerCondition.hpp>
#include <functional>

namespace NES::Statistic {

class StatisticInterface {
  public:
    /**
     * @brief Creates a request to track a specific statistic characteristic, e.g., Cardinality over car_1 in a
     * tumbling window fashion of 5 minutes
     * @param characteristic
     * @param window
     * @param triggerCondition
     * @param sendingPolicy
     * @param callBack
     */
    virtual void trackStatistic(const Characteristic& characteristic,
                                const Windowing::WindowTypePtr& window,
                                const TriggerCondition& triggerCondition,
                                const SendingPolicy& sendingPolicy,
                                std::function<void(Characteristic)>&& callBack) = 0;

    /**
     * @brief Creates a request to probe a specific statistic and returns the statistic in a ProbeResult
     * @param characteristic
     * @param period
     * @param granularity
     * @param estimationAllowed
     * @param aggFunction
     * @return ProbeResult
     */
    virtual ProbeResult<> probeStatistic(const Characteristic& characteristic,
                                         const Windowing::TimeMeasure& period,
                                         const Windowing::TimeMeasure& granularity,
                                         const bool& estimationAllowed,
                                         std::function<ProbeResult<>(ProbeResult<>)>&& aggFunction) = 0;
};

}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTIC_STATISTICINTERFACE_HPP_
