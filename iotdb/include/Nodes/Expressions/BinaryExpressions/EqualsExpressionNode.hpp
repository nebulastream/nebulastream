#ifndef NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_EQUALSEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_EQUALSEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/BinaryExpressions/LogicalBinaryExpressionNode.hpp>
namespace NES {

class EqualsExpressionNode : public LogicalBinaryExpressionNode {
  public:
    static ExpressionNodePtr create(const ExpressionNodePtr left, const ExpressionNodePtr right);
    EqualsExpressionNode();
    ~EqualsExpressionNode() = default;
    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;

};

}

#endif //NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_EQUALSEXPRESSIONNODE_HPP_
