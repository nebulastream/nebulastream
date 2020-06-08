#ifndef TOPDOWN_HPP
#define TOPDOWN_HPP

#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <stack>

namespace NES {

using namespace std;

class TopDownStrategy : public BasePlacementStrategy {

  public:
    ~TopDownStrategy() = default;

    GlobalExecutionPlanPtr initializeExecutionPlan(QueryPlanPtr queryPlan, StreamCatalogPtr streamCatalog);

    static std::unique_ptr<TopDownStrategy> create(NESTopologyPlanPtr nesTopologyPlan, GlobalExecutionPlanPtr executionPlan) {
        return std::make_unique<TopDownStrategy>(TopDownStrategy(nesTopologyPlan, executionPlan));
    }

  private:
    TopDownStrategy(NESTopologyPlanPtr nesTopologyPlan, GlobalExecutionPlanPtr executionPlan);

    /**
     * @brief place query operators and prepare the nes execution plan
     * @param queryId : the id of the query whose operators need to be placed
     * @param sinkOperator :  sink operator for the query
     * @param nesSourceNodes : list of physical source nodes
     */
    void placeOperators(std::string queryId, LogicalOperatorNodePtr sinkOperator, vector<NESTopologyEntryPtr> nesSourceNodes);
};

}// namespace NES
#endif//TOPDOWN_HPP
