
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Nodes/Expressions/LogicalExpressions/LogicalUnaryExpressionNode.hpp>

namespace NES {

LogicalUnaryExpressionNode::LogicalUnaryExpressionNode() : UnaryExpressionNode(DataTypeFactory::createBoolean()), LogicalExpressionNode() {}

LogicalUnaryExpressionNode::LogicalUnaryExpressionNode(LogicalUnaryExpressionNode* other) : UnaryExpressionNode(other) {}

bool LogicalUnaryExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<LogicalUnaryExpressionNode>()) {
        auto other = rhs->as<LogicalUnaryExpressionNode>();
        return this->getChildren()[0]->equal(other->getChildren()[0]);
    }
    return false;
}
}// namespace NES