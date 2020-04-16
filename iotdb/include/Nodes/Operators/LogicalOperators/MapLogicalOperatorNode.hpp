#ifndef MAP_LOGICAL_OPERATOR_NODE_HPP
#define MAP_LOGICAL_OPERATOR_NODE_HPP

#include <memory>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>

namespace NES {
class MapLogicalOperatorNode : public LogicalOperatorNode {
  public:
    MapLogicalOperatorNode(const ExpressionNodePtr mapExpression);
    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;

  private:
    ExpressionNodePtr mapExpression;
};
}

#endif  // MAP_LOGICAL_OPERATOR_NODE_HPP
