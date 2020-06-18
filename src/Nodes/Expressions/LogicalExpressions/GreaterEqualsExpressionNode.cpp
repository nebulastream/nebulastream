
#include <Common/DataTypes/DataType.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
namespace NES {
GreaterEqualsExpressionNode::GreaterEqualsExpressionNode() : LogicalBinaryExpressionNode(){};
ExpressionNodePtr GreaterEqualsExpressionNode::create(const ExpressionNodePtr left,
                                                      const ExpressionNodePtr right) {
    auto greaterThen = std::make_shared<GreaterEqualsExpressionNode>();
    greaterThen->setChildren(left, right);
    return greaterThen;
}

bool GreaterEqualsExpressionNode::equal(const NodePtr rhs) const {
    if (this->isIdentical(rhs)) {
        return true;
    }

    return rhs->instanceOf<GreaterEqualsExpressionNode>();
}
const std::string GreaterEqualsExpressionNode::toString() const {
    return "GreaterThenNode(" + stamp->toString() + ")";
}

}// namespace NES