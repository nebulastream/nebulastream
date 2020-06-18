
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Nodes/Expressions/LogicalExpressions/LogicalBinaryExpressionNode.hpp>
namespace NES {
LogicalBinaryExpressionNode::LogicalBinaryExpressionNode() : BinaryExpressionNode(DataTypeFactory::createBoolean()), LogicalExpressionNode() {}

}// namespace NES