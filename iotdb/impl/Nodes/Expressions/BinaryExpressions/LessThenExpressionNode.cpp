
#include <Nodes/Expressions/BinaryExpressions/LessThenExpressionNode.hpp>
namespace NES {
LessThenExpressionNode::LessThenExpressionNode(const NES::ExpressionNodePtr left,
                                           const NES::ExpressionNodePtr right) :
    LogicalBinaryExpressionNode(
        left,
        right) {}
bool LessThenExpressionNode::equal(const NodePtr rhs) const {
    return false;
}
const std::string LessThenExpressionNode::toString() const {
    return std::__cxx11::string();
}

}