
#include <Common/DataTypes/DataType.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
namespace NES {
ExpressionNodePtr EqualsExpressionNode::create(const ExpressionNodePtr left, const ExpressionNodePtr right) {
    auto equals = std::make_shared<EqualsExpressionNode>();
    equals->setChildren(left, right);
    return equals;
}

EqualsExpressionNode::EqualsExpressionNode() : LogicalBinaryExpressionNode() {}

bool EqualsExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<EqualsExpressionNode>()) {
        auto other = rhs->as<EqualsExpressionNode>();
        return this->getLeft()->equal(other->getLeft()) && this->getRight()->equal(other->getRight());
    }
    return false;
}

const std::string EqualsExpressionNode::toString() const {
    return "EqualsNode(" + stamp->toString() + ")";
}

}// namespace NES
