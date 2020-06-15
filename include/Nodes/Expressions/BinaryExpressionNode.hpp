#ifndef NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSION_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSION_HPP_
#include <Nodes/Expressions/ExpressionNode.hpp>
namespace NES {
/**
 * @brief A binary expression is represents expressions with two children.
 */
class BinaryExpressionNode : public ExpressionNode {
  public:
    BinaryExpressionNode(DataTypePtr stamp);
    ~BinaryExpressionNode() = default;

    /**
     * @brief set the children node of this expression.
     */
    void setChildren(const ExpressionNodePtr left, const ExpressionNodePtr right);

    /**
     * @brief gets the left children.
     */
    ExpressionNodePtr getLeft() const;

    /**
     * @brief gets the right children.
     */
    ExpressionNodePtr getRight() const;
};

}// namespace NES
#endif//NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSION_HPP_
