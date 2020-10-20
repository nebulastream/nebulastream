#ifndef NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSION_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSION_HPP_
#include <Nodes/Expressions/ExpressionNode.hpp>
namespace NES {
/**
 * @brief A binary expression is represents expressions with two children.
 */
class BinaryExpressionNode : public ExpressionNode {
  public:
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

    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() override = 0;

  protected:
    explicit BinaryExpressionNode(DataTypePtr stamp);
    explicit BinaryExpressionNode(BinaryExpressionNode* other);

};

}// namespace NES
#endif//NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSION_HPP_
