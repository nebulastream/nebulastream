#ifndef NES_INCLUDE_NODES_EXPRESSIONS_FIELDASSIGNMENTEXPRESSION_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_FIELDASSIGNMENTEXPRESSION_HPP_
#include <Nodes/Expressions/BinaryExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
namespace NES {
/**
 * @brief A FieldAssignmentExpression represents the assignment of an expression result to a specific field.
 */
class FieldAssignmentExpressionNode : public BinaryExpressionNode {
  public:
    FieldAssignmentExpressionNode(DataTypePtr stamp);

    /**
     * @brief Create untyped field read.
     */
    static ExpressionNodePtr create(FieldAccessExpressionNodePtr fieldAccess, ExpressionNodePtr expressionNodePtr);

    const std::string toString() const override;
    bool equal(const NodePtr rhs) const override;

    const std::string getFieldName();

  private:
    /**
     * @brief Name of the field want to access.
     */
    std::string fieldName;
};
}

#endif // NES_INCLUDE_NODES_EXPRESSIONS_FIELDASSIGNMENTEXPRESSION_HPP_
