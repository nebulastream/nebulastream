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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_IFCOPSTRATEGY_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_IFCOPSTRATEGY_HPP_

#include <Optimizer/QueryPlacementAddition/BasePlacementAdditionStrategy.hpp>

namespace NES {

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

namespace Optimizer {

using IdToIteratorIndexMapping = std::map<std::pair<OperatorId, uint64_t>, std::pair<uint64_t, uint64_t>>;

class IFCOPStrategy : public BasePlacementAdditionStrategy {

  public:
    static BasePlacementStrategyPtr create(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                           const TopologyPtr& topology,
                                           const TypeInferencePhasePtr& typeInferencePhase,
                                           PlacementAmendmentMode placementAmendmentMode);

    ~IFCOPStrategy() override = default;

    PlacementAdditionResult updateGlobalExecutionPlan(SharedQueryId sharedQueryId,
                                                      const std::set<LogicalOperatorPtr>& pinnedUpStreamNodes,
                                                      const std::set<LogicalOperatorPtr>& pinnedDownStreamNodes,
                                                      DecomposedQueryPlanVersion querySubPlanVersion) override;

  private:
    IFCOPStrategy(const GlobalExecutionPlanPtr& globalExecutionPlan,
                  const TopologyPtr& topology,
                  const TypeInferencePhasePtr& typeInferencePhase,
                  PlacementAmendmentMode placementAmendmentMode);

    /**
     * Get an random operator placement candidate
     * @param queryPlan query plan to place
     * @return a 2D matrix that represent an operator placement candidate
     */
    PlacementMatrix getPlacementCandidate(NES::QueryPlanPtr queryPlan);

    /**
     * Get the total cost of a placement candidate, which is is a weighted sum between the network cost and node over-utilization
     * cost. Specifically: totalCost = costRatio*networkCost + (1-costRatio)*overUtilizationCost
     *
     * @param placementCandidate matrix representation of the candidate
     * @param queryPlan query plan to place
     * @param costRatio ratio between network cost and node overutilization cost
     * @return total cost of the current candidate
     */
    double getCost(const PlacementMatrix& placementCandidate, NES::QueryPlanPtr queryPlan, double costRatio);

    /**
     * Get local cost, i.e., cost of placing operators in the current topology node
     * The local cost is a product of all multiplication between a decision to place operator i in the topology node j
     * with the data modification factor of the operator to place. Specifically, the local cost is computed as follows:
     *
     * localCost = Product(Placement[i][j] * DMF[i]) + (1-Placement[i][j].
     *
     * We add a third term to avoid a 0 value, which can lead to 0 cost if one of the placement decision is 0.
     *
     * @param nodePlacement vector representation of placement in a topology node
     * @param queryPlan query plan to place
     * @return local cost of placement in the topology node
     */
    double getLocalCost(const std::vector<bool>& nodePlacement, NES::QueryPlanPtr queryPlan);

    /**
     * Get the network cost of a placement candidate
     * The network cost is computed by recursively multiplying the local cost with the sum of all local cost of the child of the
     * current topology node. This process starts from the sink node and ends in the source node (i.e., nodes without any child
     * node). Specifically, the network cost is computed as follows:
     *
     * NetworkCost = localCost * Sum(cost(c)), with c as all child node of the current topology
     *
     * @param currentNode current topology node to traverse
     * @param placementCandidate matrix representation of the candidate
     * @param queryPlan query plan to place
     * @return the network cost of a placement candidate
     */
    double
    getNetworkCost(const TopologyNodePtr currentNode, const PlacementMatrix& placementCandidate, NES::QueryPlanPtr queryPlan);

    /**
     * @brief assign all remaining operator, including the sink to the placementCandidate
     * @param queryPlan query plan to place
     * @param topoIdx index of the topology node in a DFS iterator where the last assignment is made
     * @param idToIteratorIndexMapping mapping to help obtaining the index of a topology and operator node based on their ids
     * @param placedOperatorIds vector that track the id of the operator that has been placed
     * @param placementCandidate the operator placement candidate to which the assignment is to be applied
     */
    void assignRemainingOperator(NES::QueryPlanPtr queryPlan,
                                 uint32_t topoIdx,
                                 IdToIteratorIndexMapping& idToIteratorIndexMapping,
                                 std::vector<OperatorId>& placedOperatorIds,
                                 PlacementMatrix& placementCandidate);

    /**
     * @brief initialize the mapping in the topologyNodeIdToIndexMap
     */
    void initiateWorkerIdToIndexMap();

    // a mapping between Ids of nodes in the topology to its index in a depth first search iterator
    std::map<WorkerId, uint64_t> topologyNodeIdToIndexMap;
};
}// namespace Optimizer
}// namespace NES
#endif// NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_IFCOPSTRATEGY_HPP_
