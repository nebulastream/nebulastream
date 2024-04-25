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
#include <Identifiers/Identifiers.hpp>
#include <StatisticCollection/Characteristic/Characteristic.hpp>
namespace NES::Statistic {

/**
 * @brief Represents an infrastructure characteristic that results in collecting statistics for a given node/worker
 */
class InfrastructureStatistic : public Characteristic {
  public:
    /**
     * @brief Creates a InfrastructureStatistic
     * @param type: What type of metric, i.e., selectivity, cardinality, data distribution, ...
     * @param nodeId: Id of the node to collect the statistic
     * @return CharacteristicPtr
     */
    static CharacteristicPtr create(MetricPtr type, WorkerId nodeId);

    /**
     * @brief Checks for equality
     * @param rhs
     * @return True, if equal otherwise false
     */
    bool operator==(const Characteristic& rhs) const override;

    /**
     * @brief Implementing a hash method
     * @return Hash
     */
    size_t hash() const override;

    /**
     * @brief Creates a string representation
     * @return std::string
     */
    std::string toString() const override;

    /**
     * @brief Gets the nodeId
     * @return WorkerId
     */
    WorkerId getNodeId() const;

  private:
    /**
     * @brief Creates an InfrastructureCharacteristic
     * @param type
     * @param nodeId
     */
    InfrastructureStatistic(const MetricPtr type, const WorkerId nodeId);

    WorkerId nodeId;
};
}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTIC_CHARACTERISTIC_INFRASTRUCTURECHARACTERISTIC_HPP_
