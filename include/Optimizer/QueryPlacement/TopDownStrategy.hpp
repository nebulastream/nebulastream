#ifndef TOPDOWN_HPP
#define TOPDOWN_HPP

#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <stack>

namespace NES {

class TopDownStrategy : public BasePlacementStrategy {

  public:
    ~TopDownStrategy() = default;

    bool updateGlobalExecutionPlan(QueryPlanPtr queryPlan);

    static std::unique_ptr<TopDownStrategy> create(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology, TypeInferencePhasePtr typeInferencePhase,
                                                   StreamCatalogPtr streamCatalog);

  private:
    TopDownStrategy(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology, TypeInferencePhasePtr typeInferencePhase,
                    StreamCatalogPtr streamCatalog);

    /**
     * @brief place query operators and prepare the global execution plan
     * @param queryPlan: query plan to place
     * @throws exception if the operator can't be placed.
     */
    void placeQueryPlan(QueryPlanPtr queryPlan);

    /**
     * @brief Try to place input operator on the input topology node
     * @param queryId :  the query id
     * @param operatorNode : the input operator to place
     * @param candidateTopologyNode : the candidate topology node to place operator on
     */
    void placeOperator(QueryId queryId, OperatorNodePtr operatorNode, TopologyNodePtr candidateTopologyNode);

    /**
     * @brief Get topology node where all parent operators of the input operator are placed
     * @param candidateOperator: the input operator
     * @return vector of topology nodes where parent operator was placed or empty if not all parent operators are placed
     */
    std::vector<TopologyNodePtr> getTopologyNodesForParentOperators(OperatorNodePtr candidateOperator);

    /**
     * @brief Get topology node where all children operators of the input operator are to be placed
     * @param candidateOperator: the input operator
     * @return vector of topology nodes where child operator are to be placed
     */
    std::vector<TopologyNodePtr> getTopologyNodesForSourceOperators(OperatorNodePtr candidateOperator);

    /**
     * @brief Get the candidate query plan where input operator is to be appended
     * @param queryId : the query id
     * @param candidateOperator : the candidate operator
     * @param executionNode : the execution node where operator is to be placed
     * @return the query plan to which the input operator is to be appended
     */
    QueryPlanPtr addOperatorToCandidateQueryPlan(QueryId queryId, OperatorNodePtr candidateOperator, ExecutionNodePtr executionNode);
};

}// namespace NES
#endif//TOPDOWN_HPP
