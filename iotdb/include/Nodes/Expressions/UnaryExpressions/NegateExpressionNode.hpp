#ifndef NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_EQUALSEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_EQUALSEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/UnaryExpressions/LogicalUnaryExpressionNode.hpp>
namespace NES {
class NegateExpressionNode : LogicalUnaryExpressionNode {
  public:
    NegateExpressionNode(const ExpressionNodePtr child);
    ~NegateExpressionNode() = default;

    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
};
std::shared_ptr<NegateExpressionNode> createNegateNode(ExpressionNodePtr child);
}

#endif //NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_EQUALSEXPRESSIONNODE_HPP_
