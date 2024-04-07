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

#ifndef NES_NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_STATISTICCOLLECTION_PHYSICALSYNOPSISBUILDOPERATOR_HPP_
#define NES_NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_STATISTICCOLLECTION_PHYSICALSYNOPSISBUILDOPERATOR_HPP_
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicy.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistics/Metrics/Metric.hpp>

namespace NES::QueryCompilation::PhysicalOperators {

/**
 * @brief Physical operator for building a synopsis. This operator is meant as a parent class and the specific
 * synopsis building operators should extend this class. It merely stores all common members across all synopsis
 */
class PhysicalSynopsisBuildOperator {
  public:
    PhysicalSynopsisBuildOperator(const std::string_view nameOfFieldToTrack,
                                  const Statistic::MetricHash metricHash,
                                  const Windowing::WindowTypePtr windowType,
                                  const Statistic::SendingPolicyPtr sendingPolicy);

    const std::string& getNameOfFieldToTrack() const;
    Statistic::MetricHash getMetricHash() const;
    const Windowing::WindowTypePtr& getWindowType() const;
    const Statistic::SendingPolicyPtr& getSendingPolicy() const;

  protected:
    const std::string nameOfFieldToTrack;
    const Statistic::MetricHash metricHash;
    const Windowing::WindowTypePtr windowType;
    const Statistic::SendingPolicyPtr sendingPolicy;
};
} // namespace NES::QueryCompilation::PhysicalOperators
#endif//NES_NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_STATISTICCOLLECTION_PHYSICALSYNOPSISBUILDOPERATOR_HPP_
