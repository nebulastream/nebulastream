#ifndef NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_LOGICALBINARYEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_LOGICALBINARYEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/UnaryExpressions/UnaryExpressionNode.hpp>
namespace NES {
class LogicalUnaryExpressionNode : UnaryExpressionNode {
  protected:
    LogicalUnaryExpressionNode(const ExpressionNodePtr child);
};
}

#endif //NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_LOGICALBINARYEXPRESSIONNODE_HPP_
