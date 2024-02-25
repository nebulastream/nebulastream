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

#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicyLazy.hpp>
#include <Operators/LogicalOperators/StatisticCollection/TriggerCondition/NeverTrigger.hpp>
#include <StatisticCollection/StatisticCoordinator.hpp>

namespace NES::Statistic {
void StatisticCoordinator::trackStatistic(const CharacteristicPtr&,
                                          const Windowing::WindowTypePtr&,
                                          const TriggerCondition&,
                                          const SendingPolicyPtr&,
                                          std::function<void(CharacteristicPtr)>&&) {}

void StatisticCoordinator::trackStatistic(const CharacteristicPtr& characteristic, const Windowing::WindowTypePtr& window) {
    trackStatistic(characteristic, window, SENDING_LAZY);
}

void StatisticCoordinator::trackStatistic(const CharacteristicPtr& characteristic,
                                          const Windowing::WindowTypePtr& window,
                                          const SendingPolicyPtr& sendingPolicy) {
    trackStatistic(characteristic, window, NeverTrigger(), sendingPolicy, nullptr);
}

ProbeResult<> StatisticCoordinator::probeStatistic(const CharacteristicPtr&,
                                                   const Windowing::TimeMeasure&,
                                                   const Windowing::TimeMeasure&,
                                                   const bool&,
                                                   std::function<ProbeResult<>(ProbeResult<>)>&& aggFunction) {
    return aggFunction(ProbeResult<>());
}
ProbeResult<> StatisticCoordinator::probeStatistic(const CharacteristicPtr& characteristic,
                                                   const Windowing::TimeMeasure& period,
                                                   const Windowing::TimeMeasure& granularity,
                                                   const bool& estimationAllowed) {
    return probeStatistic(characteristic, period, granularity, estimationAllowed, [](const ProbeResult<>& probeResult) {
        return probeResult;
    });
}

}// namespace NES::Statistic