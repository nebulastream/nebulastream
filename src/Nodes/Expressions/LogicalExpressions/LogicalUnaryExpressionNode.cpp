
#include <Nodes/Expressions/LogicalExpressions/LogicalUnaryExpressionNode.hpp>
#include <DataTypes/DataTypeFactory.hpp>

namespace NES {

LogicalUnaryExpressionNode::LogicalUnaryExpressionNode() : UnaryExpressionNode(DataTypeFactory::createBoolean()), LogicalExpressionNode() {
}

}// namespace NES