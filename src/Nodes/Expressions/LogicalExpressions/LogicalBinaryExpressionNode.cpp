
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Nodes/Expressions/LogicalExpressions/LogicalBinaryExpressionNode.hpp>
namespace NES {
LogicalBinaryExpressionNode::LogicalBinaryExpressionNode() : BinaryExpressionNode(DataTypeFactory::createBoolean()), LogicalExpressionNode() {}

bool LogicalBinaryExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<LogicalBinaryExpressionNode>()) {
        auto other = rhs->as<LogicalBinaryExpressionNode>();
        return this->getLeft()->equal(other->getLeft()) && this->getRight()->equal(other->getRight());
    }
    return false;
}

}// namespace NES