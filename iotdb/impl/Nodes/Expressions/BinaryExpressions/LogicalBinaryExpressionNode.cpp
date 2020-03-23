
#include <Nodes/Expressions/BinaryExpressions/LogicalBinaryExpressionNode.hpp>
namespace NES {
LogicalBinaryExpressionNode::LogicalBinaryExpressionNode(const NES::ExpressionNodePtr left,
                                                         const NES::ExpressionNodePtr right) :
    BinaryExpressionNode(
        createDataType(BasicType::BOOLEAN),
        left,
        right) {}

}