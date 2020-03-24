#ifndef NES_INCLUDE_NODES_EXPRESSIONS_UNARYEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_UNARYEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/ExpressionNode.hpp>
namespace NES {
/**
 * @brief A unary expression is used to represent expressions with one child.
 */
class UnaryExpressionNode : public ExpressionNode {
  protected:
    UnaryExpressionNode(DataTypePtr stamp);

    /**
     * @brief set the child node of this expression.
     * @param child ExpressionNodePtr
     */
    void setChild(ExpressionNodePtr child);

    /**
     * @brief returns the child of this expression
     * @return
     */
    ExpressionNodePtr child() const;
};
}

#endif //NES_INCLUDE_NODES_EXPRESSIONS_UNARYEXPRESSIONNODE_HPP_
