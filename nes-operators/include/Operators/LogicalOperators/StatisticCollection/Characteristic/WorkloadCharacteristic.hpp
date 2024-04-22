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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTIC_CHARACTERISTIC_WORKLOADCHARACTERISTIC_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTIC_CHARACTERISTIC_WORKLOADCHARACTERISTIC_HPP_
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Characteristic/Characteristic.hpp>
namespace NES::Statistic {

/**
 * @brief Represents a workload characteristic that results in collecting statistics for an operator in a query
 */
class WorkloadCharacteristic : public Characteristic {
  public:
    /**
     * @brief Creates a WorkloadCharacteristic
     * @param type: What type of metric, i.e., selectivity, cardinality, data distribution, ...
     * @param queryId: Query to collect the statistics for
     * @param operatorId: Operator to collect the statistics for
     * @return CharacteristicPtr
     */
    static CharacteristicPtr create(MetricPtr type, QueryId queryId, OperatorId operatorId);

    /**
     * @brief Gets the queryId
     * @return QueryId
     */
    QueryId getQueryId() const;

    /**
     * @brief Gets the operatorId
     * @return OperatorId
     */
    OperatorId getOperatorId() const;

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

  private:
    /**
     * @brief Creates a WorkloadCharacteristic
     * @param type
     * @param queryId
     * @param operatorId
     */
    WorkloadCharacteristic(MetricPtr type, QueryId queryId, OperatorId operatorId);

    QueryId queryId;
    OperatorId operatorId;
};
}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTIC_CHARACTERISTIC_WORKLOADCHARACTERISTIC_HPP_
