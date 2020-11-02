#ifndef NES_INCLUDE_NODES_EXPRESSIONS_CONSTANTVALUEEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_CONSTANTVALUEEXPRESSIONNODE_HPP_

#include <Nodes/Expressions/ExpressionNode.hpp>
namespace NES {

class ValueType;
typedef std::shared_ptr<ValueType> ValueTypePtr;

/**
 * @brief This expression node represents a constant value and a fixed data type.
 * Thus the samp of this expression is always fixed.
 */
class ConstantValueExpressionNode : public ExpressionNode {
  public:
    /**
     * @brief Factory method to create a ConstantValueExpressionNode.
     */
    static ExpressionNodePtr create(const ValueTypePtr constantValue);

    /**
     * @brief Returns the constant value.
     */
    ValueTypePtr getConstantValue() const;

    /**
     * @brief On a constant value expression infer stamp has not to perform any action as its result type is always constant.
     * @param schema
     */
    void inferStamp(SchemaPtr schema) override;

    /**
     * @brief Creates a string of the value and the type.
     * @return
     */
    const std::string toString() const override;

    /**
     * @brief Compares if another node is equal to this constant value expression.
     * @param otherNode
     * @return true if they are equal
     */
    bool equal(const NodePtr otherNode) const override;

    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() override;

  protected:
    explicit ConstantValueExpressionNode(ConstantValueExpressionNode* other);

  private:
    explicit ConstantValueExpressionNode(const ValueTypePtr constantValue);
    // Value of this expression
    ValueTypePtr constantValue;
};

}// namespace NES

#endif//NES_INCLUDE_NODES_EXPRESSIONS_CONSTANTVALUEEXPRESSIONNODE_HPP_
