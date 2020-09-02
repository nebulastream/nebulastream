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
    void placeOperator(QueryId queryId, OperatorNodePtr candidateOperator, TopologyNodePtr candidateTopologyNode);
    std::vector<TopologyNodePtr> getTopologyNodesForParentOperators(OperatorNodePtr candidateOperator);
    QueryPlanPtr getCandidateQueryPlan(QueryId queryId, OperatorNodePtr candidateOperator, ExecutionNodePtr executionNode);
    std::vector<TopologyNodePtr> getTopologyNodesForSourceOperators(OperatorNodePtr candidateOperator);
};

}// namespace NES
#endif//TOPDOWN_HPP
