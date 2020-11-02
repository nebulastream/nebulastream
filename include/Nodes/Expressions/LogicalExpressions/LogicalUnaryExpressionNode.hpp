#ifndef NES_INCLUDE_NODES_EXPRESSIONS_UNARYEXPRESSIONS_LOGICALUNARYEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_UNARYEXPRESSIONS_LOGICALUNARYEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/LogicalExpressions/LogicalExpressionNode.hpp>
#include <Nodes/Expressions/UnaryExpressionNode.hpp>
namespace NES {
/**
 * @brief This node represents a logical unary expression.
 */
class LogicalUnaryExpressionNode : public UnaryExpressionNode, public LogicalExpressionNode {
  protected:
    LogicalUnaryExpressionNode();

  public:
    bool equal(const NodePtr rhs) const override;

    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() = 0;

  protected:
    explicit LogicalUnaryExpressionNode(LogicalUnaryExpressionNode* other);
};
}// namespace NES

#endif//NES_INCLUDE_NODES_EXPRESSIONS_UNARYEXPRESSIONS_LOGICALUNARYEXPRESSIONNODE_HPP_
