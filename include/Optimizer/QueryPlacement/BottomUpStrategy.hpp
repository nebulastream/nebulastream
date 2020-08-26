#ifndef BOTTOMUP_HPP
#define BOTTOMUP_HPP

#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <iostream>

namespace NES {

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

/**\brief:
 *          This class implements Bottom Up placement strategy. In this strategy, the source and sink operators are
 *          placed at respective nes nodes but rest of the operators are placed starting near to the source and then
 *          if the resources are not available they are placed on a node neighbouring to the node or one level up.
 */
class BottomUpStrategy : public BasePlacementStrategy {
  public:
    ~BottomUpStrategy(){};

    bool updateGlobalExecutionPlan(QueryPlanPtr queryPlan);

    static std::unique_ptr<BottomUpStrategy> create(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology,
                                                    TypeInferencePhasePtr typeInferencePhase, StreamCatalogPtr streamCatalog);

  private:
    explicit BottomUpStrategy(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology, TypeInferencePhasePtr typeInferencePhase,
                              StreamCatalogPtr streamCatalog);

    /**
     * This method is responsible for placing the operators to the nes nodes and generating ExecutionNodes.
     * @param sourceOperator : sensor nodes which act as the source source.
     * @param sourceNodes : sensor nodes which act as the source source.
     *
     * @throws exception if the operator can't be placed anywhere.
     */
    void placeOperators(QueryPlanPtr queryPlan, std::map<std::string, std::vector<TopologyNodePtr>> mapOfSourceToTopologyNodes);

    bool placeOperator(QueryId queryId, OperatorNodePtr candidateOperator, TopologyNodePtr candidateTopologyNode, std::map<uint64_t, ExecutionNodePtr>& operatorToExecutionNodeMap);
};
}// namespace NES

#endif//BOTTOMUP_HPP
