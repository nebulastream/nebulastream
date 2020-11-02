
#include <Common/DataTypes/DataType.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
namespace NES {
GreaterEqualsExpressionNode::GreaterEqualsExpressionNode() : LogicalBinaryExpressionNode(){};

GreaterEqualsExpressionNode::GreaterEqualsExpressionNode(GreaterEqualsExpressionNode* other) : LogicalBinaryExpressionNode(other) {}

ExpressionNodePtr GreaterEqualsExpressionNode::create(const ExpressionNodePtr left,
                                                      const ExpressionNodePtr right) {
    auto greaterThen = std::make_shared<GreaterEqualsExpressionNode>();
    greaterThen->setChildren(left, right);
    return greaterThen;
}

bool GreaterEqualsExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<GreaterEqualsExpressionNode>()) {
        auto other = rhs->as<GreaterEqualsExpressionNode>();
        return this->getLeft()->equal(other->getLeft()) && this->getRight()->equal(other->getRight());
    }
    return false;
}

const std::string GreaterEqualsExpressionNode::toString() const {
    return "GreaterThenNode(" + stamp->toString() + ")";
}
ExpressionNodePtr GreaterEqualsExpressionNode::copy() {
    return std::make_shared<GreaterEqualsExpressionNode>(GreaterEqualsExpressionNode(this));
}

}// namespace NES