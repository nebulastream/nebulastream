#include <Nodes/Expressions/UnaryExpressions/UnaryExpressionNode.hpp>

namespace NES {
UnaryExpressionNode::UnaryExpressionNode(DataTypePtr stamp, const ExpressionNodePtr child) : ExpressionNode(stamp) {
    addChild(child);
}
}