
#include <Nodes/Expressions/UnaryExpressions/LogicalUnaryExpressionNode.hpp>

namespace NES {

LogicalUnaryExpressionNode::LogicalUnaryExpressionNode() :
    UnaryExpressionNode(createDataType(BasicType::BOOLEAN)) {
}

}