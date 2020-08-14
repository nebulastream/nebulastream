#ifndef WINDOW_REFINEMENT_OPERATOR_CENTRAL_WINDOW_NODE_HPP
#define WINDOW_REFINEMENT_OPERATOR_CENTRAL_WINDOW_NODE_HPP

#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/WindowLogicalOperatorNode.hpp>

namespace NES {

class WindowDefinition;
typedef std::shared_ptr<WindowDefinition> WindowDefinitionPtr;

class CentralWindowOperator : public WindowLogicalOperatorNode {
  public:
    CentralWindowOperator(const WindowDefinitionPtr& windowDefinition);
};

}// namespace NES
#endif// WINDOW_LOGICAL_OPERATOR_NODE_HPP
