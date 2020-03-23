#ifndef NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_LOGICALBINARYEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_LOGICALBINARYEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/BinaryExpressions/BinaryExpressionNode.hpp>
namespace NES {
class LogicalBinaryExpressionNode : BinaryExpressionNode {
  protected:
    LogicalBinaryExpressionNode(const ExpressionNodePtr left, const ExpressionNodePtr right);

};
}

#endif //NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_LOGICALBINARYEXPRESSIONNODE_HPP_
