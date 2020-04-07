#ifndef TOPDOWN_HPP
#define TOPDOWN_HPP

#include <stack>
#include <Optimizer/NESPlacementOptimizer.hpp>

namespace NES {

class Operator;
typedef std::shared_ptr<Operator> OperatorPtr;

using namespace std;

class TopDownStrategy : public NESPlacementOptimizer {

  public:
    TopDownStrategy() = default;
    ~TopDownStrategy() = default;

    NESExecutionPlanPtr initializeExecutionPlan(InputQueryPtr inputQuery,
                                                NESTopologyPlanPtr nesTopologyPlanPtr) override;

  private:

    /**
     * @brief place query operators and prepare the nes execution plan
     * @param executionPlanPtr : the execution plan that need to be prepared
     * @param sinkOperator :  sink operator for the query
     * @param nesSourceNodes : list of physical source nodes
     * @param nesTopologyGraphPtr :  nes topology graph
     */
    void placeOperators(NESExecutionPlanPtr executionPlanPtr, OperatorPtr sinkOperator,
                        vector<NESTopologyEntryPtr> nesSourceNodes, NESTopologyGraphPtr nesTopologyGraphPtr);

    /**
     * @brief add query operator to existing execution node
     * @param operatorPtr : operator to add
     * @param executionNode : execution node to which operator need to be added
     */
    void addOperatorToExistingNode(OperatorPtr operatorPtr, ExecutionNodePtr executionNode) const;

    /**
     * @brief create new execution nesNode for the query operator.
     * @param executionPlanPtr : execution plan where new execution node will be created
     * @param operatorPtr : operator to be added
     * @param nesNode : physical node used for creating the execution node
     */
    void createNewExecutionNode(NESExecutionPlanPtr executionPlanPtr, OperatorPtr operatorPtr,
                                NESTopologyEntryPtr nesNode) const;

    NESPlacementStrategyType getType() {
        return TopDown;
    }
};

}
#endif //TOPDOWN_HPP
