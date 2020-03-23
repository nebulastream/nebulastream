#include <Nodes/Expressions/BinaryExpressions/BinaryExpressionNode.hpp>

namespace NES {
BinaryExpressionNode::BinaryExpressionNode(DataTypePtr stamp,
                                           const ExpressionNodePtr left,
                                           const ExpressionNodePtr right) : ExpressionNode(stamp) {
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