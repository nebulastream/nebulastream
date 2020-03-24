#include <Nodes/Expressions/BinaryExpressions/UnaryExpressionNode.hpp>

namespace NES {
BinaryExpressionNode::BinaryExpressionNode(DataTypePtr stamp) : ExpressionNode(stamp) {}

void BinaryExpressionNode::setChildren(const ExpressionNodePtr left, const ExpressionNodePtr right) {
    addChild(left);
    addChild(right);
}

ExpressionNodePtr BinaryExpressionNode::getLeft() const {
    return children[0]->as<ExpressionNode>();
}

ExpressionNodePtr BinaryExpressionNode::getRight() const {
    return children[1]->as<ExpressionNode>();
}

}