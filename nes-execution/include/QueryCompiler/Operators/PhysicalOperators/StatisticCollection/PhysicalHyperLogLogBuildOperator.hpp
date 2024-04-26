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

#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_STATISTICCOLLECTION_PHYSICALHYPERLOGLOGBUILDOPERATOR_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_STATISTICCOLLECTION_PHYSICALHYPERLOGLOGBUILDOPERATOR_HPP_
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicy.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Metrics/StatisticMetric.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/StatisticCollection/PhysicalSynopsisBuildOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators {
class PhysicalHyperLogLogBuildOperator : public PhysicalSynopsisBuildOperator, public PhysicalUnaryOperator, public AbstractEmitOperator {
  public:

    /**
     * @brief Creates a PhysicalCountMinBuildOperator
     * @param id
     * @param statisticId
     * @param inputSchema
     * @param outputSchema
     * @param nameOfFieldToTrack
     * @param width
     * @param metricHash
     * @param windowType
     * @param sendingPolicy
     * @return PhysicalOperatorPtr
     */
    static PhysicalOperatorPtr create(const OperatorId id,
                                      const StatisticId statisticId,
                                      const SchemaPtr& inputSchema,
                                      const SchemaPtr& outputSchema,
                                      const std::string& nameOfFieldToTrack,
                                      const uint64_t width,
                                      const Statistic::StatisticMetricHash metricHash,
                                      const Windowing::WindowTypePtr windowType,
                                      const Statistic::SendingPolicyPtr sendingPolicy);

    /**
     * @brief Creates a PhysicalCountMinBuildOperator and sets the operatorId to the nextOperatorId
     * @param statisticId
     * @param inputSchema
     * @param outputSchema
     * @param nameOfFieldToTrack
     * @param width
     * @param metricHash
     * @param windowType
     * @param sendingPolicy
     * @return PhysicalOperatorPtr
     */
    static PhysicalOperatorPtr create(const StatisticId statisticId,
                                      const SchemaPtr& inputSchema,
                                      const SchemaPtr& outputSchema,
                                      const std::string& nameOfFieldToTrack,
                                      const uint64_t width,
                                      const Statistic::StatisticMetricHash metricHash,
                                      const Windowing::WindowTypePtr windowType,
                                      const Statistic::SendingPolicyPtr sendingPolicy);

    OperatorPtr copy() override;
    uint64_t getWidth() const;

  private:
    PhysicalHyperLogLogBuildOperator(const OperatorId id,
                                     const StatisticId statisticId,
                                     const SchemaPtr& inputSchema,
                                     const SchemaPtr& outputSchema,
                                     const std::string& nameOfFieldToTrack,
                                     const uint64_t width,
                                     const Statistic::StatisticMetricHash metricHash,
                                     const Windowing::WindowTypePtr windowType,
                                     const Statistic::SendingPolicyPtr sendingPolicy);

    const uint64_t width;
};
} // namespace NES::QueryCompilation::PhysicalOperators
#endif // NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_STATISTICCOLLECTION_PHYSICALHYPERLOGLOGBUILDOPERATOR_HPP_
