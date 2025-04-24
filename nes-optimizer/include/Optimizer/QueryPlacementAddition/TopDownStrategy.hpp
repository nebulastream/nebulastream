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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_TOPDOWNSTRATEGY_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_TOPDOWNSTRATEGY_HPP_

#include <Optimizer/QueryPlacementAddition/BasePlacementAdditionStrategy.hpp>
#include <stack>

namespace NES::Optimizer {

class TopDownStrategy : public BasePlacementAdditionStrategy {

  public:
    ~TopDownStrategy() override = default;

    static BasePlacementStrategyPtr create(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                           const TopologyPtr& topology,
                                           const TypeInferencePhasePtr& typeInferencePhase,
                                           PlacementAmendmentMode placementAmendmentMode);

    PlacementAdditionResult updateGlobalExecutionPlan(SharedQueryId sharedQueryId,
                                                      const std::set<LogicalOperatorPtr>& pinnedUpStreamOperators,
                                                      const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators,
                                                      DecomposedQueryPlanVersion querySubPlanVersion,
                                                      FaultToleranceType faultTolerance,
                                                      CheckpointStorageType checkpointStorage) override;

  private:
    TopDownStrategy(const GlobalExecutionPlanPtr& globalExecutionPlan,
                    const TopologyPtr& topology,
                    const TypeInferencePhasePtr& typeInferencePhase,
                    PlacementAmendmentMode placementAmendmentMode);

    /**
     * @brief place query operators and prepare the global execution plan
     * @param pinnedUpStreamOperators: pinned upstream operators
     * @param pinnedDownStreamOperators: pinned downstream operators
     * @throws exception if the operator can't be placed.
     */
    void pinOperators(const std::set<LogicalOperatorPtr>& pinnedUpStreamOperators,
                      const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators);

    /**
     * @brief Try to place input operator on the input topology node
     * @param logicalOperator : the input operator to place
     * @param candidateTopologyNode : the candidate topology node to place operator on
     */
    void identifyPinningLocation(const LogicalOperatorPtr& logicalOperator,
                                 TopologyNodePtr candidateTopologyNode,
                                 const std::set<LogicalOperatorPtr>& pinnedUpStreamOperators);

    /**
     * @brief Get topology node where all parent operators of the input operator are placed
     * @param candidateOperator: the input operator
     * @return vector of topology nodes where parent operator was placed or empty if not all parent operators are placed
     */
    std::vector<TopologyNodePtr> getTopologyNodesForDownStreamOperators(const LogicalOperatorPtr& candidateOperator);

    /**
     * @brief Get topology node where all children operators of the input operator are to be placed
     * @param candidateOperator: the input operator
     * @return vector of topology nodes where child operator are to be placed
     */
    std::vector<TopologyNodePtr> getTopologyNodesForUpStreamOperators(const LogicalOperatorPtr& candidateOperator);
};

}// namespace NES::Optimizer
#endif// NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_TOPDOWNSTRATEGY_HPP_
