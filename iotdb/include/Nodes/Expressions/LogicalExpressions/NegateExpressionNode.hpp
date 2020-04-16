#ifndef NES_INCLUDE_NODES_EXPRESSIONS_UNARYEXPRESSIONS_NEGATEEXPRESSINNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_UNARYEXPRESSIONS_NEGATEEXPRESSINNODE_HPP_
#include <Nodes/Expressions/LogicalExpressions/LogicalUnaryExpressionNode.hpp>
namespace NES {

/**
 * @brief This node negates its child expression.
 */
class NegateExpressionNode : public LogicalUnaryExpressionNode {
  public:
    NegateExpressionNode();
    ~NegateExpressionNode() = default;

    /**
     * @brief Create a new negate expression
     */
    static ExpressionNodePtr create(const ExpressionNodePtr child);

    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
};
}

#endif //NES_INCLUDE_NODES_EXPRESSIONS_UNARYEXPRESSIONS_NEGATEEXPRESSINNODE_HPP_
