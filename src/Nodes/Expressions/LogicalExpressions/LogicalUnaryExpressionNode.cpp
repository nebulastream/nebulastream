
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Nodes/Expressions/LogicalExpressions/LogicalUnaryExpressionNode.hpp>

namespace NES {

LogicalUnaryExpressionNode::LogicalUnaryExpressionNode() : UnaryExpressionNode(DataTypeFactory::createBoolean()), LogicalExpressionNode() {
}

}// namespace NES