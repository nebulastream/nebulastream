/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_OPTIMIZER_QUERY_PLACEMENT_BOTTOM_UP_STRATEGY_HPP_
#define NES_INCLUDE_OPTIMIZER_QUERY_PLACEMENT_BOTTOM_UP_STRATEGY_HPP_

#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <iostream>

namespace NES {

class StreamCatalog;
using StreamCatalogPtr = std::shared_ptr<StreamCatalog>;
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

    bool updateGlobalExecutionPlan(QueryPlanPtr queryPlan) override;

    /**
     * This function overload is responsible for partial placement of a query.
     * The QueryPlan supplied must have NetworkSource Operators as its leaf operators and NetworkSink Operators as its root operators.
     * NetworkSource Operators can be located on arbitrary topology nodes. All NetworkSink operators must be located on same topology node.
     * Only one root node is currently allowed.
     * @param queryPlan
     * @param sourceNodes
     * @param rootNode
     * @return
     */
    bool updateGlobalExecutionPlan(QueryPlanPtr queryPlan, std::vector<TopologyNodePtr> sourceNodes, TopologyNodePtr rootNode);

    static std::unique_ptr<BottomUpStrategy> create(GlobalExecutionPlanPtr globalExecutionPlan,
                                                    TopologyPtr topology,
                                                    TypeInferencePhasePtr typeInferencePhase,
                                                    StreamCatalogPtr streamCatalog);

  private:
    explicit BottomUpStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                              TopologyPtr topology,
                              TypeInferencePhasePtr typeInferencePhase,
                              StreamCatalogPtr streamCatalog);

    /**
     * This method is responsible for placing the operators to the nes nodes and generating ExecutionNodes.
     * @param queryPlan: query plan to place
     * @throws exception if the operator can't be placed.
     */
    void placeQueryPlanOnTopology(const QueryPlanPtr& queryPlan, bool partialPlacement = false);

    /**
     * @brief Try to place input operator on the input topology node
     * @param queryId :  the query id
     * @param operatorNode : the input operator to place
     * @param candidateTopologyNode : the candidate topology node to place operator on
     */
    void placeOperatorOnTopologyNode(QueryId queryId, const OperatorNodePtr& operatorNode, TopologyNodePtr candidateTopologyNode, bool partialPlacement = false);

    /**
     * @brief Get topology node where all children operators of the input operator are placed
     * @param operatorNode: the input operator
     * @return vector of topology nodes where child operator was placed or empty if not all children operators are placed
     */
    std::vector<TopologyNodePtr> getTopologyNodesForChildrenOperators(const OperatorNodePtr& operatorNode);

    /**
     * @brief Get the candidate query plan where input operator is to be appended
     * @param queryId : the query id
     * @param operatorNode : the candidate operator
     * @param executionNode : the execution node where operator is to be placed
     * @return the query plan to which the input operator is to be appended
     */
    static QueryPlanPtr
    getCandidateQueryPlan(QueryId queryId, const OperatorNodePtr& operatorNode, const ExecutionNodePtr& executionNode);
};
}// namespace NES::Optimizer

#endif// NES_INCLUDE_OPTIMIZER_QUERY_PLACEMENT_BOTTOM_UP_STRATEGY_HPP_
