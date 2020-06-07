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

    static std::unique_ptr<BottomUpStrategy> create(NESTopologyPlanPtr nesTopologyPlan) {
        return std::make_unique<BottomUpStrategy>(BottomUpStrategy(nesTopologyPlan));
    }

  private:

    explicit BottomUpStrategy(NESTopologyPlanPtr nesTopologyPlan);

    /**
     * This method is responsible for placing the operators to the nes nodes and generating ExecutionNodes.
     * @param sourceOperator : sensor nodes which act as the source source.
     * @param sourceNodes : sensor nodes which act as the source source.
     *
     * @return: Partially completed Execution plan for input query.
     * @throws exception if the operator can't be placed anywhere.
     */
    GlobalExecutionPlanPtr placeOperators(std::string queryId, LogicalOperatorNodePtr sourceOperator, vector<NESTopologyEntryPtr> sourceNodes);

    /**
     * @brief Finds all the nodes that can be used for performing FWD operator
     * @param sourceNodes
     * @param rootNode
     * @return vector of worker nodes for placement of FWD operator
     */
    std::vector<NESTopologyEntryPtr> getCandidateNodesForFwdOperatorPlacement(const vector<NESTopologyEntryPtr>& sourceNodes,
                                                                              const NESTopologyEntryPtr rootNode) const;
};
}// namespace NES

#endif//BOTTOMUP_HPP
