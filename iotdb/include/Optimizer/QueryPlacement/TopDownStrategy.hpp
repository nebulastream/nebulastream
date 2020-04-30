#ifndef TOPDOWN_HPP
#define TOPDOWN_HPP

#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <stack>

namespace NES {

using namespace std;

class TopDownStrategy : public BasePlacementStrategy {

  public:
    ~TopDownStrategy() = default;

    NESExecutionPlanPtr initializeExecutionPlan(QueryPtr query, NESTopologyPlanPtr nesTopologyPlanPtr) override;

    static std::unique_ptr<TopDownStrategy> create() {
        return std::make_unique<TopDownStrategy>(TopDownStrategy());
    }

  private:
    TopDownStrategy() = default;

    /**
     * @brief place query operators and prepare the nes execution plan
     * @param executionPlanPtr : the execution plan that need to be prepared
     * @param sinkOperator :  sink operator for the query
     * @param nesSourceNodes : list of physical source nodes
     * @param nesTopologyGraphPtr :  nes topology graph
     */
    void placeOperators(NESExecutionPlanPtr executionPlanPtr, LogicalOperatorNodePtr sinkOperator,
                        vector<NESTopologyEntryPtr> nesSourceNodes, NESTopologyGraphPtr nesTopologyGraphPtr);

    /**
     * @brief add query operator to existing execution node
     * @param operatorPtr : operator to add
     * @param executionNode : execution node to which operator need to be added
     */
    void addOperatorToExistingNode(LogicalOperatorNodePtr operatorPtr, ExecutionNodePtr executionNode) const;

    /**
     * @brief create new execution nesNode for the query operator.
     * @param executionPlanPtr : execution plan where new execution node will be created
     * @param operatorPtr : operator to be added
     * @param nesNode : physical node used for creating the execution node
     */
    void createNewExecutionNode(NESExecutionPlanPtr executionPlanPtr, LogicalOperatorNodePtr operatorPtr,
                                NESTopologyEntryPtr nesNode) const;

    /**
     * @brief Finds all the nodes that can be used for performing FWD operator
     * @param sourceNodes
     * @param rootNode
     * @return
     */
    vector<NESTopologyEntryPtr> getCandidateNodesForFwdOperatorPlacement(const vector<NESTopologyEntryPtr>& sourceNodes,
                                                                         const NESTopologyEntryPtr rootNode) const;
};

}// namespace NES
#endif//TOPDOWN_HPP
