
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
namespace NES {
LessEqualsExpressionNode::LessEqualsExpressionNode() : LogicalBinaryExpressionNode(){};
ExpressionNodePtr LessEqualsExpressionNode::create(const ExpressionNodePtr left,
                                                   const ExpressionNodePtr right) {
    auto lessThen = std::make_shared<LessEqualsExpressionNode>();
    lessThen->setChildren(left, right);
    return lessThen;
}

bool LessEqualsExpressionNode::equal(const NodePtr rhs) const {
    if (this->isIdentical(rhs)) {
        return true;
    }

    return rhs->instanceOf<LessEqualsExpressionNode>();
}
const std::string LessEqualsExpressionNode::toString() const {
    return "LessThenNode(" + stamp->toString() + ")";
}

}// namespace NES