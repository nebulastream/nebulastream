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

#ifndef NES_INCLUDE_OPTIMIZER_QUERYPLACEMENT_TOPDOWNSTRATEGY_HPP_
#define NES_INCLUDE_OPTIMIZER_QUERYPLACEMENT_TOPDOWNSTRATEGY_HPP_

#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <stack>

namespace NES::Optimizer {

class TopDownStrategy : public BasePlacementStrategy {

  public:
    ~TopDownStrategy() override = default;

    static BasePlacementStrategyPtr create(GlobalExecutionPlanPtr globalExecutionPlan,
                                           TopologyPtr topology,
                                           TypeInferencePhasePtr typeInferencePhase);

    bool updateGlobalExecutionPlan(QueryId queryId,
                                   FaultToleranceType faultToleranceType,
                                   LineageType lineageType,
                                   const std::vector<OperatorNodePtr>& pinnedUpStreamOperators,
                                   const std::vector<OperatorNodePtr>& pinnedDownStreamOperators) override;

  private:
    TopDownStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                    TopologyPtr topology,
                    TypeInferencePhasePtr typeInferencePhase);

    /**
     * @brief place query operators and prepare the global execution plan
     * @param queryPlan: query plan to place
     * @throws exception if the operator can't be placed.
     */
    void performOperatorPlacement(QueryId queryId,
                                  const std::vector<OperatorNodePtr>& pinnedUpStreamOperators,
                                  const std::vector<OperatorNodePtr>& pinnedDownStreamOperators);

    /**
     * @brief Try to place input operator on the input topology node
     * @param pinnedUpStreamOperator :  the query id
     * @param operatorNode : the input operator to place
     * @param candidateTopologyNode : the candidate topology node to place operator on
     */
    void placeOperator(QueryId pinnedUpStreamOperator,
                       const OperatorNodePtr& operatorNode,
                       TopologyNodePtr candidateTopologyNode,
                       const std::vector<OperatorNodePtr>& pinnedUpStreamOperators);

    /**
     * @brief Get topology node where all parent operators of the input operator are placed
     * @param candidateOperator: the input operator
     * @return vector of topology nodes where parent operator was placed or empty if not all parent operators are placed
     */
    std::vector<TopologyNodePtr> getTopologyNodesForParentOperators(const OperatorNodePtr& candidateOperator);

    /**
     * @brief Get topology node where all children operators of the input operator are to be placed
     * @param candidateOperator: the input operator
     * @return vector of topology nodes where child operator are to be placed
     */
    std::vector<TopologyNodePtr> getTopologyNodesForChildOperators(const OperatorNodePtr& candidateOperator);

    /**
     * @brief Get the candidate query plan where input operator is to be appended
     * @param queryId : the query id
     * @param candidateOperator : the candidate operator
     * @param executionNode : the execution node where operator is to be placed
     * @return the query plan to which the input operator is to be appended
     */
    static QueryPlanPtr addOperatorToCandidateQueryPlan(QueryId queryId,
                                                        const OperatorNodePtr& candidateOperator,
                                                        const ExecutionNodePtr& executionNode);
};

}// namespace NES::Optimizer
#endif// NES_INCLUDE_OPTIMIZER_QUERYPLACEMENT_TOPDOWNSTRATEGY_HPP_
