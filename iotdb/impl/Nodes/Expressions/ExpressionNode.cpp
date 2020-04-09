#include <Nodes/Expressions/ExpressionNode.hpp>

namespace NES {
ExpressionNode::ExpressionNode(DataTypePtr stamp) : stamp(stamp) {
}

bool ExpressionNode::isPredicate() {
    return stamp->isEqual(createDataType(BasicType::BOOLEAN));
}

DataTypePtr ExpressionNode::getStamp() {
    return stamp;
}
}