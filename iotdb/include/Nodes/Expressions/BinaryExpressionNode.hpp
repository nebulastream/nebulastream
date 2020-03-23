#ifndef NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSION_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSION_HPP_
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/BinaryOperationType.hpp>
namespace NES {
class BinaryExpressionNode;
typedef std::shared_ptr<BinaryExpressionNode> BinaryExpressionNodePtr;

class BinaryExpressionNode : public ExpressionNode {
  public:
    BinaryExpressionNode(DataTypePtr stamp, const BinaryOperationType operationType, const ExpressionNodePtr left, const ExpressionNodePtr right);

    ExpressionNodePtr getLeft() const {
        return children[0]->as<ExpressionNode>();
    }

    ExpressionNodePtr getRight() const {
        return children[1]->as<ExpressionNode>();
    }

  protected:
    const BinaryOperationType operationType;
};

}
#endif //NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSION_HPP_
