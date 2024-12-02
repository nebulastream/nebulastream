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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_BOTTOMUPSTRATEGY_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_BOTTOMUPSTRATEGY_HPP_

#include <Optimizer/QueryPlacementAddition/BasePlacementAdditionStrategy.hpp>
#include <iostream>

namespace NES::Catalogs::Source {

class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace NES::Catalogs::Source

namespace NES::Optimizer {

/**\brief:
 *          This class implements Bottom Up placement strategy. In this strategy, the source and sink operators are
 *          placed at respective nes nodes but rest of the operators are placed starting near to the source and then
 *          if the resources are not available they are placed on a node neighbouring to the node or one level up.
 */
class BottomUpStrategy : public BasePlacementAdditionStrategy {
  public:
    ~BottomUpStrategy() override = default;

    static BasePlacementStrategyPtr create(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                           const TopologyPtr& topology,
                                           const TypeInferencePhasePtr& typeInferencePhase,
                                           PlacementAmendmentMode placementAmendmentMode,
                                           FaultToleranceType faultTolerance);

    PlacementAdditionResult updateGlobalExecutionPlan(SharedQueryId sharedQueryId,
                                                      const std::set<LogicalOperatorPtr>& pinnedUpStreamOperators,
                                                      const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators,
                                                      DecomposedQueryPlanVersion querySubPlanVersion) override;

    TopologyNodePtr findCandidateTopologyNode(TopologyNodePtr startNode);

    explicit BottomUpStrategy(const GlobalExecutionPlanPtr& globalExecutionPlan,
                              const TopologyPtr& topology,
                              const TypeInferencePhasePtr& typeInferencePhase,
                              PlacementAmendmentMode placementAmendmentMode,
                              FaultToleranceType faultTolerance);

  private:
    /**
     * This method is responsible for pinning the operators to the worker nodes.
     * @param pinnedUpStreamOperators: pinned upstream operators
     * @param pinnedDownStreamNodes: pinned downstream operators
     * @throws exception if the operator can't be placed.
     */
    void pinOperators(const std::set<LogicalOperatorPtr>& pinnedUpStreamOperators,
                      const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators);

    /**
     * @brief Try to pin input operator on the input topology node otherwise find appropriate node to pin the operator
     * @param logicalOperator : the input operator to place
     * @param candidateTopologyNode : the candidate topology node to place operator on
     * @param pinnedDownStreamOperators: list of pinned downstream node after which placement stops
     */
    void identifyPinningLocation(const LogicalOperatorPtr& logicalOperator,
                                 TopologyNodePtr candidateTopologyNode,
                                 const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators);
};
}// namespace NES::Optimizer

#endif// NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_BOTTOMUPSTRATEGY_HPP_
