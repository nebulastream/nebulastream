#ifndef NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSION_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSION_HPP_
#include <Nodes/Expressions/ExpressionNode.hpp>
namespace NES {

class BinaryExpressionNode : public ExpressionNode {
  public:
    BinaryExpressionNode(DataTypePtr stamp);
    ~BinaryExpressionNode() = default;

    void setChildren(const ExpressionNodePtr left, const ExpressionNodePtr right);

    ExpressionNodePtr getLeft() const;
    ExpressionNodePtr getRight() const;
};

}
#endif //NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSION_HPP_
