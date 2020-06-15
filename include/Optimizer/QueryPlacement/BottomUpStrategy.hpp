#ifndef BOTTOMUP_HPP
#define BOTTOMUP_HPP

#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <iostream>

namespace NES {
class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

using namespace std;

/**\brief:
 *          This class implements Bottom Up placement strategy. In this strategy, the source and sink operators are
 *          placed at respective nes nodes but rest of the operators are placed starting near to the source and then
 *          if the resources are not available they are placed on a node neighbouring to the node or one level up.
 */
class BottomUpStrategy : public BasePlacementStrategy {
  public:
    ~BottomUpStrategy(){};

    GlobalExecutionPlanPtr initializeExecutionPlan(QueryPlanPtr queryPlan, StreamCatalogPtr streamCatalog);

    static std::unique_ptr<BottomUpStrategy> create(NESTopologyPlanPtr nesTopologyPlan, GlobalExecutionPlanPtr executionPlan) {
        return std::make_unique<BottomUpStrategy>(BottomUpStrategy(nesTopologyPlan, executionPlan));
    }

  private:
    explicit BottomUpStrategy(NESTopologyPlanPtr nesTopologyPlan, GlobalExecutionPlanPtr globalExecutionPlan);

    /**
     * This method is responsible for placing the operators to the nes nodes and generating ExecutionNodes.
     * @param sourceOperator : sensor nodes which act as the source source.
     * @param sourceNodes : sensor nodes which act as the source source.
     *
     * @throws exception if the operator can't be placed anywhere.
     */
    void placeOperators(std::string queryId, LogicalOperatorNodePtr sourceOperator, vector<NESTopologyEntryPtr> sourceNodes);
};
}// namespace NES

#endif//BOTTOMUP_HPP
