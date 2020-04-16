
#include <Nodes/Expressions/LogicalExpressions/LogicalUnaryExpressionNode.hpp>

namespace NES {

LogicalUnaryExpressionNode::LogicalUnaryExpressionNode() :
    UnaryExpressionNode(createDataType(BasicType::BOOLEAN)) {
}

}