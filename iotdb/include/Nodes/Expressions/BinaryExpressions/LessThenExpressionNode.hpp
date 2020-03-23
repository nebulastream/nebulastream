#ifndef NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_EQUALSEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_EQUALSEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/BinaryExpressions/LogicalBinaryExpressionNode.hpp>
namespace NES {
class LessThenExpressionNode : LogicalBinaryExpressionNode {
    LessThenExpressionNode(const ExpressionNodePtr left, const ExpressionNodePtr right);
    ~LessThenExpressionNode() = default;
    
    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
};
}

#endif //NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_EQUALSEXPRESSIONNODE_HPP_
