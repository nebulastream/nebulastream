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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_PHASES_STATISTICIDINFERENCEPHASE_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_PHASES_STATISTICIDINFERENCEPHASE_HPP_

#include <memory>
#include <unordered_set>
namespace NES {

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class Operator;
using OperatorPtr = std::shared_ptr<Operator>;

class DecomposedQueryPlan;
using DecomposedQueryPlanPtr = std::shared_ptr<DecomposedQueryPlan>;

namespace Optimizer {

class StatisticIdInferencePhase;
using StatisticIdInferencePhasePtr = std::shared_ptr<StatisticIdInferencePhase>;

/**
 * @brief This phase iterates over all operators, except sources, and sets a unique statistic id for each logical operator
 */
class StatisticIdInferencePhase {
  public:
    /**
     * @brief Factory method to create a StatisticIdInferencePhase
     * @return StatisticIdInferencePhasePtr
     */
    static StatisticIdInferencePhasePtr create();

    /**
     * @brief Apply the rule to the Query plan. After this step, all operators will have a unique statisticId
     * @param queryPlanPtr : The original query plan
     * @return The updated query plan
     */
    QueryPlanPtr execute(QueryPlanPtr queryPlan);

    /**
     * @brief Apply the rule to the Query plan. After this step, all operators will have a unique statisticId
     * @param decomposedQueryPlan: The original query plan
     * @return The updated query plan
     */
    DecomposedQueryPlanPtr execute(DecomposedQueryPlanPtr decomposedQueryPlan);

  private:
    /**
     * @brief Private constructor for creating a StatisticIdInferencePhase
     */
    explicit StatisticIdInferencePhase();

    /**
     * @brief Performs inference by iterating over all operators and setting a new statistic id for each operator.
     * We do not set a statistic Id for the sources, as they still have to be expanded into multiple physical ones.
     * We do this in LogicalSourceExpansionRule
     * @param allOperators
     */
    void performInference(std::unordered_set<OperatorPtr> allOperators);

};

}// namespace Optimizer
}// namespace NES

#endif // NES_OPTIMIZER_INCLUDE_OPTIMIZER_PHASES_STATISTICIDINFERENCEPHASE_HPP_
