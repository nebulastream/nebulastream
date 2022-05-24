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

#ifndef NES_INCLUDE_OPTIMIZER_QUERYPLACEMENT_MANUALPLACEMENTSTRATEGY_HPP_
#define NES_INCLUDE_OPTIMIZER_QUERYPLACEMENT_MANUALPLACEMENTSTRATEGY_HPP_

#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>

namespace NES::Optimizer {

/**
 * @brief A placement strategy where users specify manually where to place operators in the topology
 */
class ManualPlacementStrategy : public BasePlacementStrategy {
  public:
    ~ManualPlacementStrategy() override = default;

    bool updateGlobalExecutionPlan(QueryId queryId,
                                   FaultToleranceType faultToleranceType,
                                   LineageType lineageType,
                                   const std::vector<OperatorNodePtr>& pinnedUpStreamOperators,
                                   const std::vector<OperatorNodePtr>& pinnedDownStreamOperators) override;

    static std::unique_ptr<ManualPlacementStrategy>
    create(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology, TypeInferencePhasePtr typeInferencePhase);

  private:
    explicit ManualPlacementStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                                     TopologyPtr topology,
                                     TypeInferencePhasePtr typeInferencePhase);

    /**
     * @brief Iterate through operators between pinnedUpStreamOperators and pinnedDownStreamOperators and assign them to the
     * designated topology node
     * @param queryId id of the query containing operators to place
     * @param pinnedUpStreamOperators the upstream operators preceeding operators to place
     * @param pinnedDownStreamOperators the downstream operators succeeding operators to place
     */
    void performOperatorPlacement(QueryId queryId,
                                  const std::vector<OperatorNodePtr>& pinnedUpStreamOperators,
                                  const std::vector<OperatorNodePtr>& pinnedDownStreamOperators);

    /**
     * @brief Update the shared query plan in a topology node by adding a new operator in it
     * @param queryId id of the query containing operators to place
     * @param operatorNode operator to place
     * @param candidateTopologyNode the topology containing shared query plan to update
     * @param pinnedDownStreamOperators the downstream operators succeeding operators to place
     */
    void placeOperator(QueryId queryId,
                       const OperatorNodePtr& operatorNode,
                       TopologyNodePtr candidateTopologyNode,
                       const std::vector<OperatorNodePtr>& pinnedDownStreamOperators);
};

}// namespace NES::Optimizer

#endif// NES_INCLUDE_OPTIMIZER_QUERYPLACEMENT_MANUALPLACEMENTSTRATEGY_HPP_
