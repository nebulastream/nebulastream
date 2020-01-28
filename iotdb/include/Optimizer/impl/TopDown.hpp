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

    NESExecutionPlan initializeExecutionPlan(InputQueryPtr inputQuery, NESTopologyPlanPtr nesTopologyPlanPtr) override;

  private:

    /**
     * @brief place query operators and prepare nes execution plan
     * @param nesExecutionPlan
     * @param sinkOperator
     * @param sourceNodes
     * @param nesTopologyGraphPtr
     */
    void placeOperators(NESExecutionPlan nesExecutionPlan, const OperatorPtr& sinkOperator,
                        deque<NESTopologyEntryPtr> sourceNodes, const NESTopologyGraphPtr& nesTopologyGraphPtr);

    /**
     * @brief add query operator to existing execution node
     * @param targetSource
     * @param processOperator
     * @param executionNode
     */
    void addOperatorToExistingNode(NESTopologyEntryPtr& targetSource, OperatorPtr& processOperator,
                                   const ExecutionNodePtr& executionNode) const;

    /**
     * @brief create new execution node for the query operator.
     * @param executionGraph
     * @param processOperator
     * @param node
     */
    void createNewExecutionNode(NESExecutionPlan& executionGraph, OperatorPtr& processOperator,
                                NESTopologyEntryPtr& node) const;
};

}
#endif //TOPDOWN_HPP
