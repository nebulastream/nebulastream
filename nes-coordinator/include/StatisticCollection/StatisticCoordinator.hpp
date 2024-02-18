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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTIC_STATISTICCOORDINATOR_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTIC_STATISTICCOORDINATOR_HPP_
#include <StatisticCollection/StatisticInterface.hpp>
#include <functional>
namespace NES::Statistic {

class StatisticCoordinator : public StatisticInterface {
  public:
    /**
     * @brief Implements trackStatistic from StatisticInterface
     */
    void trackStatistic(const CharacteristicPtr&,
                        const Windowing::WindowTypePtr&,
                        const TriggerCondition&,
                        const SendingPolicy&,
                        std::function<void(CharacteristicPtr)>&&) override;
    /**
     * @brief Calling trackStatistic(characteristic, window, SENDING_LAZY)
     */
    void trackStatistic(const CharacteristicPtr& characteristic, const Windowing::WindowTypePtr& window);

    /**
     * @brief Calling trackStatistic(characteristic, window, NeverTrigger(), sendingPolicy, nullptr)
     */
    void trackStatistic(const CharacteristicPtr& characteristic,
                        const Windowing::WindowTypePtr& window,
                        const SendingPolicy& sendingPolicy);

    /**
     * @brief Implements probeStatistic from StatisticInterface
     */
    ProbeResult<> probeStatistic(const CharacteristicPtr&,
                                 const Windowing::TimeMeasure&,
                                 const Windowing::TimeMeasure&,
                                 const bool&,
                                 std::function<ProbeResult<>(ProbeResult<>)>&& aggFunction) override;

    /**
     * @brief Calling probeStatistic with an aggregation function that does not change the ProbeResult
     */
    ProbeResult<> probeStatistic(const CharacteristicPtr& characteristic,
                                 const Windowing::TimeMeasure& period,
                                 const Windowing::TimeMeasure& granularity,
                                 const bool& estimationAllowed);
};

}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTIC_STATISTICCOORDINATOR_HPP_
