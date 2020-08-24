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
     * @param queryId: the id of the query whose operators need to be placed
     * @param sinkOperator:  sink operator for the query
     * @param nesSourceNodes: list of physical source nodes
     */
    void placeOperators(QueryId queryId, LogicalOperatorNodePtr sinkOperator, std::vector<TopologyNodePtr> nesSourceNodes);
};

}// namespace NES
#endif//TOPDOWN_HPP
