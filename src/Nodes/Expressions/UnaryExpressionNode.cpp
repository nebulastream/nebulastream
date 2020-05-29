#include <Nodes/Expressions/UnaryExpressionNode.hpp>

namespace NES {
UnaryExpressionNode::UnaryExpressionNode(DataTypePtr stamp) : ExpressionNode(stamp) {}

void UnaryExpressionNode::setChild(NES::ExpressionNodePtr child) {
    addChildWithEqual(child);
}
ExpressionNodePtr UnaryExpressionNode::child() const {
    return children[0]->as<ExpressionNode>();
}
}// namespace NES