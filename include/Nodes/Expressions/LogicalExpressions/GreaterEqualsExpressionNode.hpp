#ifndef NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_GREATEREQUALSEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_GREATEREQUALSEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/LogicalExpressions/LogicalBinaryExpressionNode.hpp>
namespace NES {

/**
 * @brief This node represents a greater comparision between the two children.
 */
class GreaterEqualsExpressionNode : public LogicalBinaryExpressionNode {
  public:
    GreaterEqualsExpressionNode();
    ~GreaterEqualsExpressionNode() = default;
    /**
    * @brief Create a new greater then expression
    */
    static ExpressionNodePtr create(const ExpressionNodePtr left, const ExpressionNodePtr right);
    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;

    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() override;

  protected:
    explicit GreaterEqualsExpressionNode(GreaterEqualsExpressionNode* other);
};
}// namespace NES

#endif// NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_GREATEREQUALSEXPRESSIONNODE_HPP_
