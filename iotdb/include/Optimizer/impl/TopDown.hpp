#ifndef TOPDOWN_HPP
#define TOPDOWN_HPP

#include <Operators/Operator.hpp>
#include <stack>
#include "../NESPlacementOptimizer.hpp"

namespace NES {

using namespace std;

class TopDown : public NESPlacementOptimizer {

  public:
    TopDown() = default;
    ~TopDown() = default;

    NESExecutionPlanPtr initializeExecutionPlan(InputQueryPtr inputQuery, NESTopologyPlanPtr nesTopologyPlanPtr) override;

  private:

    /**
     * @brief place query operators and prepare nes execution plan
     * @param nesExecutionPlanPtr
     * @param sinkOperator
     * @param nesSourceNodes
     * @param nesTopologyGraphPtr
     */
    void placeOperators(NESExecutionPlanPtr nesExecutionPlanPtr, const OperatorPtr sinkOperator,
                        deque<NESTopologyEntryPtr> nesSourceNodes, const NESTopologyGraphPtr nesTopologyGraphPtr);

    /**
     * @brief add query operator to existing execution node
     * @param operatorPtr
     * @param executionNode
     */
    void addOperatorToExistingNode(OperatorPtr operatorPtr, const ExecutionNodePtr executionNode) const;

    /**
     * @brief create new execution nesNode for the query operator.
     * @param executionPlanPtr
     * @param processOperator
     * @param nesNode
     */
    void createNewExecutionNode(NESExecutionPlanPtr executionPlanPtr, OperatorPtr processOperator,
                                NESTopologyEntryPtr nesNode) const;
};

}
#endif //TOPDOWN_HPP
