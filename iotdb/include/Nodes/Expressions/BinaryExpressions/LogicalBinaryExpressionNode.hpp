#ifndef NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_LOGICALBINARYEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_LOGICALBINARYEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/BinaryExpressions/UnaryExpressionNode.hpp>
namespace NES {
class LogicalBinaryExpressionNode : public BinaryExpressionNode {
  protected:
    LogicalBinaryExpressionNode();
    ~LogicalBinaryExpressionNode() = default;

};
}

#endif //NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_LOGICALBINARYEXPRESSIONNODE_HPP_
