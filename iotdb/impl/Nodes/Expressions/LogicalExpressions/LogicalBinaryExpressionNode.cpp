
#include <Nodes/Expressions/LogicalExpressions/LogicalBinaryExpressionNode.hpp>
namespace NES {
LogicalBinaryExpressionNode::LogicalBinaryExpressionNode() :
    BinaryExpressionNode(createDataType(BasicType::BOOLEAN)), LogicalExpressionNode() {}

}