#include <Nodes/Expressions/BinaryExpressions/BinaryExpressionNode.hpp>

namespace NES {
BinaryExpressionNode::BinaryExpressionNode(DataTypePtr stamp) : ExpressionNode(stamp) {}

void BinaryExpressionNode::setChildren(const ExpressionNodePtr left, const ExpressionNodePtr right) {
    addChildWithEqual(left);
    addChildWithEqual(right);
}

ExpressionNodePtr BinaryExpressionNode::getLeft() const {
    if (children.size() != 2) {
        NES_FATAL_ERROR("A binary expression always should have two children, but it had: " << children.size());
    }
    return children[0]->as<ExpressionNode>();
}

ExpressionNodePtr BinaryExpressionNode::getRight() const {
    if (children.size() != 2) {
        NES_FATAL_ERROR("A binary expression always should have two children, but it had: " << children.size());
    }
    return children[1]->as<ExpressionNode>();
}

}