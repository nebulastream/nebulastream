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

#ifndef NES_NES_WORKER_INCLUDE_STATISTICCOLLECTION_STATISTICREQUESTS_HPP_
#define NES_NES_WORKER_INCLUDE_STATISTICCOLLECTION_STATISTICREQUESTS_HPP_

#include <StatisticCollection/Characteristic/Characteristic.hpp>
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicy.hpp>
#include <Operators/LogicalOperators/StatisticCollection/TriggerCondition/TriggerCondition.hpp>
#include <StatisticCollection/StatisticProbeHandling/ProbeExpression.hpp>
#include <StatisticCollection/Characteristic/Characteristic.hpp>
#include <Measures/TimeMeasure.hpp>
#include <Types/WindowType.hpp>
#include <Statistics/StatisticKey.hpp>
#include <sstream>
#include <functional>

namespace NES::Statistic {

/**
 * @brief Base struct for any statistic requests to be sent to the worker nodes
 */
struct StatisticRequest {
    explicit StatisticRequest(const StatisticHash& statisticHash) : statisticHash(statisticHash) {}
    const StatisticHash statisticHash;
};

/**
 * @brief Struct that represents a request to track a statistic, for now, we do not use it but we will later
 */
struct StatisticTrackRequest : public StatisticRequest {
    StatisticTrackRequest(const StatisticHash& statisticHash,
                          const CharacteristicPtr& characteristic,
                          const TriggerConditionPtr& triggerCondition,
                          const SendingPolicyPtr& sendingPolicy,
                          const Windowing::WindowTypePtr& windowType,
                          const std::function<void(CharacteristicPtr)>& callBack)
        : StatisticRequest(statisticHash), characteristic(characteristic), triggerCondition(triggerCondition),
          sendingPolicy(sendingPolicy), windowType(windowType), callBack(callBack) {}
    const CharacteristicPtr characteristic;
    const TriggerConditionPtr triggerCondition;
    const SendingPolicyPtr sendingPolicy;
    const Windowing::WindowTypePtr windowType;
    std::function<void(CharacteristicPtr)> callBack;
};

/**
 * @brief Struct that represents a request to probe a statistic
 */
struct StatisticProbeRequest : public StatisticRequest {
    explicit StatisticProbeRequest(const StatisticHash& statisticHash,
                                   const Windowing::TimeMeasure& startTs,
                                   const Windowing::TimeMeasure& endTs,
                                   const Windowing::TimeMeasure& granularity,
                                   const ProbeExpression& probeExpression)
        : StatisticRequest(statisticHash), startTs(startTs), endTs(endTs), probeExpression(probeExpression),
          granularity(granularity) {}

    explicit StatisticProbeRequest(const StatisticHash& statisticHash,
                                   const Windowing::TimeMeasure& granularity,
                                   const ProbeExpression& probeExpression) : StatisticProbeRequest(statisticHash, Windowing::TimeMeasure(0), Windowing::TimeMeasure(UINT64_MAX), granularity, probeExpression) {}
    const Windowing::TimeMeasure startTs;
    const Windowing::TimeMeasure endTs;
    const ProbeExpression probeExpression;
    const Windowing::TimeMeasure granularity;
};

/**
 * @brief Struct that represents a request to probe a statistic plus a grpc address to send the request to
 */
struct StatisticProbeRequestGRPC : public StatisticProbeRequest {
    explicit StatisticProbeRequestGRPC(const StatisticHash& statisticHash,
                                       const Windowing::TimeMeasure& startTs,
                                       const Windowing::TimeMeasure& endTs,
                                       const ProbeExpression& probeExpression,
                                       const Windowing::TimeMeasure& granularity,
                                       const WorkerId& workerId)
        : StatisticProbeRequestGRPC(StatisticProbeRequest(statisticHash, startTs, endTs, granularity, probeExpression), workerId) {}
    explicit StatisticProbeRequestGRPC(const StatisticProbeRequest& probeRequest, const WorkerId& workerId)
        : StatisticProbeRequest(probeRequest), workerId(workerId) {}

    const WorkerId workerId;
};

}// namespace NES::Statistic

#endif//NES_NES_WORKER_INCLUDE_STATISTICCOLLECTION_STATISTICREQUESTS_HPP_
