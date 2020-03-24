#ifndef NES_INCLUDE_NODES_EXPRESSIONS_CONSTANTVALUEEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_CONSTANTVALUEEXPRESSIONNODE_HPP_

#include <Nodes/Expressions/ExpressionNode.hpp>
namespace NES {
class ConstantValueExpressionNode : public ExpressionNode {
  public:
    static ExpressionNodePtr create(const ValueTypePtr constantValue);
    ConstantValueExpressionNode(const ValueTypePtr constantValue);
    ValueTypePtr getConstantValue() const;
    const std::string toString() const override;
    bool equal(const NodePtr rhs) const override;
  private:
    ValueTypePtr constantValue;


};

}

#endif //NES_INCLUDE_NODES_EXPRESSIONS_CONSTANTVALUEEXPRESSIONNODE_HPP_
