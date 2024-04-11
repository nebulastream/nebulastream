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

#include <Operators/LogicalOperators/Windows/WindowingForwardRefs.hpp>
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicy.hpp>
#include <Operators/LogicalOperators/StatisticCollection/TriggerCondition/TriggerCondition.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Characteristic/Characteristic.hpp>
#include <sstream>

namespace NES::Statistic {

/**
 * @brief Base struct for any statistic requests to be sent to the worker nodes
 */
struct StatisticRequest {
    explicit StatisticRequest(const StatisticHash& statisticHash) : statisticHash(statisticHash) {}
    const StatisticHash statisticHash;
};

/**
 * @brief Struct that represents a request to probe a statistic
 */
struct StatisticProbeRequest : public StatisticRequest {
    explicit StatisticProbeRequest(const StatisticHash& statisticHash,
                                   const Windowing::TimeMeasure& startTs,
                                   const Windowing::TimeMeasure& endTs,
                                   const ProbeExpression& probeExpression)
        : StatisticRequest(statisticHash), startTs(startTs), endTs(endTs), probeExpression(probeExpression) {}
    const Windowing::TimeMeasure startTs;
    const Windowing::TimeMeasure endTs;
    const ProbeExpression probeExpression;
};

/**
 * @brief Struct that represents a request to probe a statistic plus a grpc address to send the request to
 */
struct StatisticProbeRequestGRPC : public StatisticProbeRequest {
    explicit StatisticProbeRequestGRPC(const StatisticHash& statisticHash,
                                       const Windowing::TimeMeasure& startTs,
                                       const Windowing::TimeMeasure& endTs,
                                       const ProbeExpression& probeExpression,
                                       const std::string& address)
        : StatisticProbeRequestGRPC(StatisticProbeRequest(statisticHash, startTs, endTs, probeExpression), address) {}
    explicit StatisticProbeRequestGRPC(const StatisticProbeRequest& probeRequest, const std::string& address)
        : StatisticProbeRequest(probeRequest), address(address) {}

    const std::string address;
};

} // namespace NES::Statistic

#endif//NES_NES_WORKER_INCLUDE_STATISTICCOLLECTION_STATISTICREQUESTS_HPP_
