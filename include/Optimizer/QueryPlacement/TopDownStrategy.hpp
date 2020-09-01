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
     * @return true if successful else false
     * @throws exception if the operator can't be placed.
     */
    bool placeOperators(QueryPlanPtr queryPlan);
    void recursiveOperatorPlacement(QueryPlanPtr candidateQueryPlan, OperatorNodePtr candidateOperator, TopologyNodePtr candidateTopologyNode, std::map<uint64_t, ExecutionNodePtr>& operatorToExecutionNodeMap);
    void placeNAryOrSinkOperator(QueryId queryId, OperatorNodePtr candidateOperator, TopologyNodePtr candidateTopologyNode, std::map<uint64_t, ExecutionNodePtr>& operatorToExecutionNodeMap);
    std::vector<TopologyNodePtr> getTopologyNodesForParentOperators(OperatorNodePtr candidateOperator, std::map<uint64_t, ExecutionNodePtr>& operatorToExecutionNodeMap);
};

}// namespace NES
#endif//TOPDOWN_HPP
