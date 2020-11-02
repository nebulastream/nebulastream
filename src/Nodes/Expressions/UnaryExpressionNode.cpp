#include <Nodes/Expressions/UnaryExpressionNode.hpp>
#include <utility>

namespace NES {
UnaryExpressionNode::UnaryExpressionNode(DataTypePtr stamp) : ExpressionNode(std::move(stamp)) {}

UnaryExpressionNode::UnaryExpressionNode(UnaryExpressionNode* other) : ExpressionNode(other) {}

void UnaryExpressionNode::setChild(ExpressionNodePtr child) {
    addChildWithEqual(child);
}
ExpressionNodePtr UnaryExpressionNode::child() const {
    return children[0]->as<ExpressionNode>();
}
}// namespace NES