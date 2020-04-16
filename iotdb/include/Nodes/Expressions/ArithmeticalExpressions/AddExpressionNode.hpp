#ifndef NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICALEXPRESSIONS_ADDEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICALEXPRESSIONS_ADDEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/ArithmeticalExpressions/ArithmeticalExpressionNode.hpp>
namespace NES {
/**
 * @brief This node represents an ADD expression.
 */
class AddExpressionNode : public ArithmeticalExpressionNode {
  public:
    AddExpressionNode(DataTypePtr stamp);
    ~AddExpressionNode() = default;
    /**
     * @brief Create a new ADD expression
     */
    static ExpressionNodePtr create(const ExpressionNodePtr left, const ExpressionNodePtr right);
    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
};

} // namespace NES

#endif // NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICALEXPRESSIONS_ADDEXPRESSIONNODE_HPP_
