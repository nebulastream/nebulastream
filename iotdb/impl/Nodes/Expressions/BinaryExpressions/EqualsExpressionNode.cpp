
#include <Nodes/Expressions/BinaryExpressions/EqualsExpressionNode.hpp>
namespace NES {
ExpressionNodePtr EqualsExpressionNode::create(const ExpressionNodePtr left, const ExpressionNodePtr right) {
    auto equals = std::make_shared<EqualsExpressionNode>();
    equals->setChildren(left, right);
    return equals;
}
EqualsExpressionNode::EqualsExpressionNode() :
    LogicalBinaryExpressionNode() {}
bool EqualsExpressionNode::equal(const NodePtr rhs) const {
    return false;
}
const std::string EqualsExpressionNode::toString() const {
    return "EqualsNode()";
}

}

