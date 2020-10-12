#ifndef WINDOW_LOGICAL_OPERATOR_NODE_HPP
#define WINDOW_LOGICAL_OPERATOR_NODE_HPP

#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>

namespace NES {

class LogicalWindowDefinition;
typedef std::shared_ptr<LogicalWindowDefinition> LogicalWindowDefinitionPtr;

class WindowLogicalOperatorNode;
typedef std::shared_ptr<WindowLogicalOperatorNode> WindowLogicalOperatorNodePtr;

class WindowLogicalOperatorNode : public LogicalOperatorNode {
  public:
    WindowLogicalOperatorNode(const LogicalWindowDefinitionPtr windowDefinition);
    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
    const LogicalWindowDefinitionPtr& getWindowDefinition() const;
    OperatorNodePtr copy() override;
    bool isIdentical(NodePtr rhs) const override;
    bool inferSchema() override;

  protected:
    LogicalWindowDefinitionPtr windowDefinition;
};

}// namespace NES
#endif// WINDOW_LOGICAL_OPERATOR_NODE_HPP
