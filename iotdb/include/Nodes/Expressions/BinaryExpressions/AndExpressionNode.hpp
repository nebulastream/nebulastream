#ifndef NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_ANDEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_ANDEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/BinaryExpressions/LogicalBinaryExpressionNode.hpp>
namespace NES {
class AndExpressionNode : public LogicalBinaryExpressionNode {
  public:
    AndExpressionNode();
    ~AndExpressionNode() = default;
    static ExpressionNodePtr create(const ExpressionNodePtr left, const ExpressionNodePtr right);
    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
};
}

#endif //NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_ANDEXPRESSIONNODE_HPP_
