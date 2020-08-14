#ifndef WINDOW_REFINEMENT_OPERATOR_DISTRIBUTED_WINDOW__NODE_HPP
#define WINDOW_REFINEMENT_OPERATOR_DISTRIBUTED_WINDOW__NODE_HPP

#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/WindowLogicalOperatorNode.hpp>

namespace NES {

class WindowDefinition;
typedef std::shared_ptr<WindowDefinition> WindowDefinitionPtr;

class DistributedWindowOperator : public WindowLogicalOperatorNode {
  public:
    DistributedWindowOperator(const WindowDefinitionPtr& windowDefinition);
};

}// namespace NES
#endif// WINDOW_LOGICAL_OPERATOR_NODE_HPP
