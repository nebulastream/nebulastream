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

};
}

#endif // NES_INCLUDE_NODES_EXPRESSIONS_FIELDASSIGNMENTEXPRESSION_HPP_
