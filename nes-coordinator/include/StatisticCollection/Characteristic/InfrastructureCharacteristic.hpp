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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTIC_CHARACTERISTIC_INFRASTRUCTURECHARACTERISTIC_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTIC_CHARACTERISTIC_INFRASTRUCTURECHARACTERISTIC_HPP_
#include <StatisticCollection/Characteristic/Characteristic.hpp>
#include <Identifiers.hpp>
namespace NES::Statistic {

/**
 * @brief Represents an infrastructure characteristic that results in collecting statistics for a given node/worker
 */
class InfrastructureStatistic : public Characteristic {
  public:
    /**
     * @brief Creates an InfrastructureCharacteristic
     * @param type
     * @param nodeId
     */
    InfrastructureStatistic(Metric type, WorkerId nodeId) : Characteristic(type), nodeId(nodeId) {}

    /**
     * @brief Gets the nodeId
     * @return WorkerId
     */
    WorkerId getNodeId() const { return nodeId; }

  private:
    WorkerId nodeId;
};
} // namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTIC_CHARACTERISTIC_INFRASTRUCTURECHARACTERISTIC_HPP_
