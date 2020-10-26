#ifndef NES_INCLUDE_NODES_EXPRESSIONS_FIELDASSIGNMENTEXPRESSION_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_FIELDASSIGNMENTEXPRESSION_HPP_
#include <Nodes/Expressions/BinaryExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
namespace NES {

class FieldAssignmentExpressionNode;
typedef std::shared_ptr<FieldAssignmentExpressionNode> FieldAssignmentExpressionNodePtr;
/**
 * @brief A FieldAssignmentExpression represents the assignment of an expression result to a specific field.
 */
class FieldAssignmentExpressionNode : public BinaryExpressionNode {
  public:
    FieldAssignmentExpressionNode(DataTypePtr stamp);

    /**
     * @brief Create untyped field read.
     */
    static FieldAssignmentExpressionNodePtr create(FieldAccessExpressionNodePtr fieldAccess, ExpressionNodePtr expressionNodePtr);

    const std::string toString() const override;
    bool equal(const NodePtr rhs) const override;

    /**
     * @brief return the field to which a new value is assigned.
     * @return FieldAccessExpressionNodePtr
     */
    FieldAccessExpressionNodePtr getField() const;
    /**
     * @brief returns the expressions, which calculates the new value.
     * @return ExpressionNodePtr
     */
    ExpressionNodePtr getAssignment() const;

    void inferStamp(SchemaPtr schema) override;

    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() override;

  protected:
    explicit FieldAssignmentExpressionNode(FieldAssignmentExpressionNode* other);
};
}// namespace NES

#endif// NES_INCLUDE_NODES_EXPRESSIONS_FIELDASSIGNMENTEXPRESSION_HPP_
