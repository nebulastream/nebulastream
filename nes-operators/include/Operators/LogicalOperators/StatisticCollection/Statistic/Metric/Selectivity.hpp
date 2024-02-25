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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTIC_SELECTIVITY_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTIC_SELECTIVITY_HPP_
#include <Operators/LogicalOperators/StatisticCollection/Statistic/Metric/Metric.hpp>

namespace NES::Statistic {

/**
 * @brief Collects statistics to estimate the selectivity. We define the selectivity as a ratio of how many tuples
 * an operator filters out. For example, a selectivity of 75% would mean that 75% of all tuples are not passed on by the
 * operator.
 */
class Selectivity : public Metric {
  public:
    /**
     * @brief Creates a Selectivity wrapped in a MetricPtr
     * @param expressionNode
     * @return MetricPtr
     */
    static MetricPtr create(const FieldAccessExpressionNodePtr& field);

    /**
     * @brief Checks for equality
     * @param rhs
     * @return True, if equal otherwise false
     */
    bool operator==(const Metric& rhs) const override;

    /**
     * @brief Creates a string representation
     * @return String representation
     */
    std::string toString() const override;

  private:
    /**
     * @brief Private constructor for a Selectivity
     * @param expressionNode
     */
    explicit Selectivity(const FieldAccessExpressionNodePtr& field);
};

}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTIC_SELECTIVITY_HPP_
