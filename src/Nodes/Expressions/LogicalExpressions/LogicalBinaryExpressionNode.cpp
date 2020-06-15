
#include <Nodes/Expressions/LogicalExpressions/LogicalBinaryExpressionNode.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeFactory.hpp>
namespace NES {
LogicalBinaryExpressionNode::LogicalBinaryExpressionNode() : BinaryExpressionNode(DataTypeFactory::createBoolean()), LogicalExpressionNode() {}

}// namespace NES