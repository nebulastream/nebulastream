#ifndef NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICALEXPRESSIONS_DIVEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICALEXPRESSIONS_DIVEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/ArithmeticalExpressions/ArithmeticalExpressionNode.hpp>
namespace NES{

/**
 * @brief This node represents a division expression.
 */
class DivExpressionNode : public ArithmeticalExpressionNode {
  public:
    DivExpressionNode(DataTypePtr stamp);
    ~DivExpressionNode() = default;
    /**
     * @brief Create a new DIV expression
     */
    static ExpressionNodePtr create(const ExpressionNodePtr left, const ExpressionNodePtr right);
    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
};

}

#endif // NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICALEXPRESSIONS_DIVEXPRESSIONNODE_HPP_
