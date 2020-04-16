
#include <Nodes/Expressions/BinaryExpressions/GreaterEqualsExpressionNode.hpp>
namespace NES {
GreaterEqualsExpressionNode::GreaterEqualsExpressionNode() :
    LogicalBinaryExpressionNode() {};
ExpressionNodePtr GreaterEqualsExpressionNode::create(const ExpressionNodePtr left,
                                                 const ExpressionNodePtr right) {
    auto greaterThen = std::make_shared<GreaterEqualsExpressionNode>();
    greaterThen->setChildren(left, right);
    return greaterThen;
}

bool GreaterEqualsExpressionNode::equal(const NodePtr rhs) const {
    return false;
}
const std::string GreaterEqualsExpressionNode::toString() const {
    return "GreaterThenNode()";
}

}