
#include <DataTypes/DataType.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
namespace NES {
ExpressionNodePtr EqualsExpressionNode::create(const ExpressionNodePtr left, const ExpressionNodePtr right) {
    auto equals = std::make_shared<EqualsExpressionNode>();
    equals->setChildren(left, right);
    return equals;
}

EqualsExpressionNode::EqualsExpressionNode() : LogicalBinaryExpressionNode() {}

bool EqualsExpressionNode::equal(const NodePtr rhs) const {
    if (this->isIdentical(rhs)) {
        return true;
    }
    return rhs->instanceOf<EqualsExpressionNode>();
}

const std::string EqualsExpressionNode::toString() const {
    return "EqualsNode(" + stamp->toString() + ")";
}

}// namespace NES
