
#include <Common/DataTypes/DataType.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
namespace NES {
GreaterExpressionNode::GreaterExpressionNode() : LogicalBinaryExpressionNode(){};
ExpressionNodePtr GreaterExpressionNode::create(const ExpressionNodePtr left,
                                                const ExpressionNodePtr right) {
    auto greater = std::make_shared<GreaterExpressionNode>();
    greater->setChildren(left, right);
    return greater;
}

bool GreaterExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<GreaterExpressionNode>()) {
        auto other = rhs->as<GreaterExpressionNode>();
        return this->getLeft()->equal(other->getLeft()) && this->getRight()->equal(other->getRight());
    }
    return false;
}

const std::string GreaterExpressionNode::toString() const {
    return "GreaterNode(" + stamp->toString() + ")";
}

}// namespace NES