
#include <Nodes/Expressions/BinaryExpressions/LessEqualsExpressionNode.hpp>
namespace NES {
LessEqualsExpressionNode::LessEqualsExpressionNode() :
    LogicalBinaryExpressionNode() {};
ExpressionNodePtr LessEqualsExpressionNode::create(const ExpressionNodePtr left,
                                                 const ExpressionNodePtr right) {
    auto lessThen = std::make_shared<LessEqualsExpressionNode>();
    lessThen->setChildren(left, right);
    return lessThen;
}

bool LessEqualsExpressionNode::equal(const NodePtr rhs) const {
    return false;
}
const std::string LessEqualsExpressionNode::toString() const {
    return "LessThenNode()";
}

}