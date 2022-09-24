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

#ifndef NES_INCLUDE_OPTIMIZER_QUERYPLACEMENT_BOTTOMUPSTRATEGY_HPP_
#define NES_INCLUDE_OPTIMIZER_QUERYPLACEMENT_BOTTOMUPSTRATEGY_HPP_

#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <iostream>
#include <Topology/TopologyNode.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>

namespace NES {

class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace NES

namespace NES::Optimizer {

/**\brief:
 *          This class implements Bottom Up placement strategy. In this strategy, the source and sink operators are
 *          placed at respective nes nodes but rest of the operators are placed starting near to the source and then
 *          if the resources are not available they are placed on a node neighbouring to the node or one level up.
 */
class BottomUpStrategy : public BasePlacementStrategy {
  public:
    ~BottomUpStrategy() override = default;


    std::vector<ExecutionNodePtr> getNeighborNodes(ExecutionNodePtr executionNode, int levelsLower, int targetDepth);
    int getDepth(GlobalExecutionPlanPtr globalExecutionPlan, ExecutionNodePtr executionNode, int counter);
    static std::unique_ptr<BasePlacementStrategy>
    create(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology, TypeInferencePhasePtr typeInferencePhase);

    bool updateGlobalExecutionPlan(QueryId queryId,
                                   FaultToleranceType faultToleranceType,
                                   LineageType lineageType,
                                   const std::vector<OperatorNodePtr>& pinnedUpStreamOperators,
                                   const std::vector<OperatorNodePtr>& pinnedDownStreamOperators) override;

  private:
    explicit BottomUpStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                              TopologyPtr topology,
                              TypeInferencePhasePtr typeInferencePhase);



    /**
     * This method is responsible for placing the operators to the nes nodes and generating ExecutionNodes.
     * @param queryId: query id
     * @param pinnedUpStreamOperators: pinned upstream operators
     * @param pinnedDownStreamNodes: pinned downstream operators
     * @throws exception if the operator can't be placed.
     */
    void performOperatorPlacement(QueryId queryId,
                                  const std::vector<OperatorNodePtr>& pinnedUpStreamOperators,
                                  const std::vector<OperatorNodePtr>& pinnedDownStreamOperators);

    /**
     * @brief Try to place input operator on the input topology node
     * @param queryId :  the query id
     * @param operatorNode : the input operator to place
     * @param candidateTopologyNode : the candidate topology node to place operator on
     * @param pinnedDownStreamOperators: list of pinned downstream node after which placement stops
     */
    void placeOperator(QueryId queryId,
                       const OperatorNodePtr& operatorNode,
                       TopologyNodePtr candidateTopologyNode,
                       const std::vector<OperatorNodePtr>& pinnedDownStreamOperators);
    FaultToleranceType checkFaultTolerance(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology, QueryId queryId);

/**
 * returns true if there are other nodes within the globalExecutionPlan on which the specified SubQuery is not deployed.
 * @param globalExecutionPlan
 * @param queryId
 * @return
 */
    bool otherNodesAvailable(GlobalExecutionPlanPtr globalExecutionPlan, std::vector<long> topologyIds, QueryId queryId);


    uint16_t getEffectivelyUsedResources(ExecutionNodePtr exNode, TopologyPtr topology, GlobalExecutionPlanPtr globalExecutionPlan, QueryId queryId) const;

    /**
     * Sum up all effective ressources and latencies of a given GlobalExecutionPlan
     * @param globalExecutionPlan
     * @return
     */
    float calcExecutionPlanCosts(GlobalExecutionPlanPtr globalExecutionPlan);

    /**
     * Calculate a queries' effective ressources and latencies for a set of TopologyNodes
     * @param topologyNodes
     * @param queryId
     */
    void calcEffectiveValues(std::vector<TopologyNodePtr> topologyNodes, QueryId queryId);
    float getEffectiveProcessingCosts(ExecutionNodePtr executionNode);
    float calcOutputQueue(ExecutionNodePtr executionNode);
    float getNetworkConnectivity(ExecutionNodePtr upstreamNode, ExecutionNodePtr downstreamNode);
    float calcLinkWeight(ExecutionNodePtr upstreamNode, ExecutionNodePtr downstreamNode);
    float calcNetworkingCosts(GlobalExecutionPlanPtr globalExecutionPlan, QueryId queryId, ExecutionNodePtr executionNode);
    std::vector<TopologyNodePtr> getDownstreamTree(TopologyNodePtr topNode, bool bottomUp);
    float calcUpstreamBackup(ExecutionNodePtr executionNode, QueryId queryId);
    std::vector<TopologyNodePtr> getUpstreamTree(TopologyNodePtr topNode, bool reversed);
    float calcDownstreamLinkWeights(ExecutionNodePtr executionNode, QueryId queryId);
    std::tuple<float,float,float> calcActiveStandby(ExecutionNodePtr executionNode, int replicas, QueryId queryId);

    std::tuple<float, float, float> calcCheckpointing(ExecutionNodePtr executionNode);

};
}// namespace NES::Optimizer

#endif// NES_INCLUDE_OPTIMIZER_QUERYPLACEMENT_BOTTOMUPSTRATEGY_HPP_
