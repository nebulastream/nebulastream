#ifndef NES_INCLUDE_NODES_EXPRESSIONS_CONSTANTVALUEEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_CONSTANTVALUEEXPRESSIONNODE_HPP_

#include <Nodes/Expressions/ExpressionNode.hpp>
namespace NES {
/**
 * @brief This expression node represents a constant value
 */
class ConstantValueExpressionNode : public ExpressionNode {
  public:
    explicit ConstantValueExpressionNode(const ValueTypePtr constantValue);
    /**
     * @brief Factory method to create a ConstantValueExpressionNode.
     */
    static ExpressionNodePtr create(const ValueTypePtr constantValue);

    /**
     * @brief Returns the constant value.
     */
    ValueTypePtr getConstantValue() const;

    void inferStamp(SchemaPtr schema) override;

    const std::string toString() const override;
    bool equal(const NodePtr rhs) const override;
  private:
    ValueTypePtr constantValue;
};

}

#endif //NES_INCLUDE_NODES_EXPRESSIONS_CONSTANTVALUEEXPRESSIONNODE_HPP_
