
#include <Nodes/Expressions/BinaryExpressions/LogicalBinaryExpressionNode.hpp>
namespace NES {
LogicalBinaryExpressionNode::LogicalBinaryExpressionNode() :
    BinaryExpressionNode(createDataType(BasicType::BOOLEAN)) {}

}