#ifndef NES_INCLUDE_NODES_EXPRESSIONS_CONSTANTVALUEEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_CONSTANTVALUEEXPRESSIONNODE_HPP_

#include <Nodes/Expressions/ExpressionNode.hpp>
namespace NES {
class ConstantValueExpressionNode : public ExpressionNode {
    ConstantValueExpressionNode(const ValueTypePtr constantValue);
    ValueTypePtr getConstantValue() const;
  private:
    ValueTypePtr constantValue;
    bool equal(const NodePtr rhs) const override;
  public:
    const std::string toString() const override;
};

ExpressionNodePtr createConstValueExpressionNode(const ValueTypePtr constantValue);
}

#endif //NES_INCLUDE_NODES_EXPRESSIONS_CONSTANTVALUEEXPRESSIONNODE_HPP_
