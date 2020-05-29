#ifndef NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_LESSEQUALSEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_LESSEQUALSEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/LogicalExpressions/LogicalBinaryExpressionNode.hpp>
namespace NES {

/**
 * @brief This node represents a less then comparision between the two children.
 */
class LessEqualsExpressionNode : public LogicalBinaryExpressionNode {
  public:
    LessEqualsExpressionNode();
    ~LessEqualsExpressionNode() = default;
    /**
    * @brief Create a new less then expression
    */
    static ExpressionNodePtr create(const ExpressionNodePtr left, const ExpressionNodePtr right);
    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
};
}// namespace NES

#endif// NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_LESSEQUALSEXPRESSIONNODE_HPP_
