#ifndef NES_INCLUDE_NODES_EXPRESSIONS_UNARYEXPRESSIONS_LOGICALUNARYEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_UNARYEXPRESSIONS_LOGICALUNARYEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/UnaryExpressions/UnaryExpressionNode.hpp>
namespace NES {
/**
 * @brief This node represents a logical unary expression.
 */
class LogicalUnaryExpressionNode : public UnaryExpressionNode {
  protected:
    LogicalUnaryExpressionNode();
};
}

#endif //NES_INCLUDE_NODES_EXPRESSIONS_UNARYEXPRESSIONS_LOGICALUNARYEXPRESSIONNODE_HPP_
