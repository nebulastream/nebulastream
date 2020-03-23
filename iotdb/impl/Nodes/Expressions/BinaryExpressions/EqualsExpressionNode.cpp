
#include <Nodes/Expressions/BinaryExpressions/EqualsExpressionNode.hpp>
namespace NES {
EqualsExpressionNode::EqualsExpressionNode(const NES::ExpressionNodePtr left,
                                           const NES::ExpressionNodePtr right) :
    LogicalBinaryExpressionNode(
        left,
        right) {}

}