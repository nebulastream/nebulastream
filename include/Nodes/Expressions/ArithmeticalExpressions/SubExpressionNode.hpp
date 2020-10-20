#ifndef NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICALEXPRESSIONS_SUBEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICALEXPRESSIONS_SUBEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/ArithmeticalExpressions/ArithmeticalExpressionNode.hpp>
namespace NES {

/**
 * @brief This node represents a subtraction expression.
 */
class SubExpressionNode : public ArithmeticalExpressionNode {
  public:
    SubExpressionNode(DataTypePtr stamp);
    ~SubExpressionNode() = default;

    /**
     * @brief Create a new SUB expression
     */
    static ExpressionNodePtr create(const ExpressionNodePtr left, const ExpressionNodePtr right);
    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
    ExpressionNodePtr copy() override;
};

}// namespace NES

#endif// NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICALEXPRESSIONS_SUBEXPRESSIONNODE_HPP_
