#include <Nodes/Expressions/BinaryExpressionNode.hpp>
#include <utility>

namespace NES {
BinaryExpressionNode::BinaryExpressionNode(DataTypePtr stamp) : ExpressionNode(std::move(stamp)) {}

BinaryExpressionNode::BinaryExpressionNode(BinaryExpressionNode* other): ExpressionNode(other) {
    addChildWithEqual(getLeft()->copy());
    addChildWithEqual(getRight()->copy());
}


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



}// namespace NES