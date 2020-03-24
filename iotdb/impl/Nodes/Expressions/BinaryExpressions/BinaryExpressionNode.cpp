#include <Nodes/Expressions/BinaryExpressions/BinaryExpressionNode.hpp>

namespace NES {
BinaryExpressionNode::BinaryExpressionNode(DataTypePtr stamp) : ExpressionNode(stamp) {}

void BinaryExpressionNode::setChildren(const ExpressionNodePtr left, const ExpressionNodePtr right) {
    addChildWithEqual(left);
    addChildWithEqual(right);
}

ExpressionNodePtr BinaryExpressionNode::getLeft() const {
    return children[0]->as<ExpressionNode>();
}

ExpressionNodePtr BinaryExpressionNode::getRight() const {
    return children[1]->as<ExpressionNode>();
}

}