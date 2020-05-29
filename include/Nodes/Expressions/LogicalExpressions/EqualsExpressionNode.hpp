#ifndef NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_EQUALSEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_EQUALSEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/LogicalExpressions/LogicalBinaryExpressionNode.hpp>
namespace NES {

/**
 * @brief This node represents an equals comparision between the two children.
 */
class EqualsExpressionNode : public LogicalBinaryExpressionNode {
  public:
    EqualsExpressionNode();
    ~EqualsExpressionNode() = default;
    /**
    * @brief Create a new equals expression
    */
    static ExpressionNodePtr create(const ExpressionNodePtr left, const ExpressionNodePtr right);
    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
};
}// namespace NES

#endif//NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_EQUALSEXPRESSIONNODE_HPP_
