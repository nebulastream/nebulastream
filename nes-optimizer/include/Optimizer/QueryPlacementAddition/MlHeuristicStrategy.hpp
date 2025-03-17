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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_MLHEURISTICSTRATEGY_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_MLHEURISTICSTRATEGY_HPP_

#include <Optimizer/QueryPlacementAddition/BasePlacementAdditionStrategy.hpp>
#include <iostream>

namespace NES {

class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace NES

namespace NES::Optimizer {

/**
 * @brief
 *          This class implements Bottom Up placement strategy. In this strategy, the source and sink operators are
 *          placed at respective nes nodes but rest of the operators are placed starting near to the source and then
 *          if the resources are not available they are placed on a node neighbouring to the node or one level up.
 */
class MlHeuristicStrategy : public BasePlacementAdditionStrategy {
  public:
    ~MlHeuristicStrategy() override = default;

    static const bool DEFAULT_ENABLE_OPERATOR_REDUNDANCY_ELIMINATION = false;
    static const bool DEFAULT_ENABLE_CPU_SAVER_MODE = true;
    static const int DEFAULT_MIN_RESOURCE_LIMIT = 5;
    static const bool DEFAULT_LOW_THROUGHPUT_SOURCE = false;
    static const bool DEFAULT_ML_HARDWARE = false;

    PlacementAdditionResult updateGlobalExecutionPlan(SharedQueryId sharedQueryId,
                                                      const std::set<LogicalOperatorPtr>& pinnedUpStreamOperators,
                                                      const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators,
                                                      DecomposedQueryPlanVersion querySubPlanVersion) override;

    /**
     * @brief creates an Object of this class through a static create function
     * @param globalExecutionPlan
     * @param topology
     * @param typeInferencePhase
     * @return
     */
    static BasePlacementStrategyPtr create(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                           const TopologyPtr& topology,
                                           const TypeInferencePhasePtr& typeInferencePhase,
                                           PlacementAmendmentMode placementAmendmentMode);

  private:
    explicit MlHeuristicStrategy(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                 const TopologyPtr& topology,
                                 const TypeInferencePhasePtr& typeInferencePhase,
                                 PlacementAmendmentMode placementAmendmentMode);

    void performOperatorPlacement(SharedQueryId sharedQueryId,
                                  const std::set<LogicalOperatorPtr>& pinnedUpStreamOperators,
                                  const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators);

    void identifyPinningLocation(SharedQueryId sharedQueryId,
                                 const LogicalOperatorPtr& logicalOperator,
                                 TopologyNodePtr candidateTopologyNode,
                                 const std::set<LogicalOperatorPtr>& pinnedDownStreamOperator);

    /**
     * @brief checks if the ratio of #sink_fields/#source_fields > 1/product of all selectivities
     * @param operatorNode
     * @return
     */
    bool pushUpBasedOnFilterSelectivity(const LogicalOperatorPtr& operatorNode);

    /**
     * @brief removes redundant operators
     * @param sharedQueryId
     */
    void performOperatorRedundancyElimination(SharedQueryId sharedQueryId);
};
}// namespace NES::Optimizer

#endif// NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_MLHEURISTICSTRATEGY_HPP_
