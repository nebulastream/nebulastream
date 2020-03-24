#ifndef NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_LESSTHENEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_LESSTHENEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/BinaryExpressions/LogicalBinaryExpressionNode.hpp>
namespace NES {
class LessThenExpressionNode : public LogicalBinaryExpressionNode {
  public:
    LessThenExpressionNode();
    ~LessThenExpressionNode() = default;
    static ExpressionNodePtr create(const ExpressionNodePtr left, const ExpressionNodePtr right);
    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
};
}

#endif //NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_LESSTHENEXPRESSIONNODE_HPP_
