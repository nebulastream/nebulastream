
#include <Nodes/Expressions/UnaryExpressions/LogicalUnaryExpressionNode.hpp>

namespace NES {

LogicalUnaryExpressionNode::LogicalUnaryExpressionNode(const ExpressionNodePtr child) :
    UnaryExpressionNode(createDataType(BasicType::BOOLEAN), child) {
}

}