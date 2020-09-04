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
     * @param queryPlan: query plan to place
     * @throws exception if the operator can't be placed.
     */
    void placeQueryPlanOnTopology(QueryPlanPtr queryPlan);

    /**
     * @brief Try to place input operator on the input topology node
     * @param queryId :  the query id
     * @param operatorNode : the input operator to place
     * @param candidateTopologyNode : the candidate topology node to place operator on
     */
    void placeOperatorOnTopologyNode(QueryId queryId, OperatorNodePtr operatorNode, TopologyNodePtr candidateTopologyNode);

    /**
     * @brief Get topology node where all children operators of the input operator are placed
     * @param operatorNode: the input operator
     * @return vector of topology nodes where child operator was placed or empty if not all children operators are placed
     */
    std::vector<TopologyNodePtr> getTopologyNodesForChildrenOperators(OperatorNodePtr operatorNode);

    /**
     * @brief Get the candidate query plan where input operator is to be appended
     * @param queryId : the query id
     * @param operatorNode : the candidate operator
     * @param executionNode : the execution node where operator is to be placed
     * @return the query plan to which the input operator is to be appended
     */
    QueryPlanPtr getCandidateQueryPlan(QueryId queryId, OperatorNodePtr operatorNode, ExecutionNodePtr executionNode);
};
}// namespace NES

#endif//BOTTOMUP_HPP
