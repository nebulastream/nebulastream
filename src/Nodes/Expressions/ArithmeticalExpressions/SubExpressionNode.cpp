#include <Common/DataTypes/DataType.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <utility>
namespace NES {

SubExpressionNode::SubExpressionNode(DataTypePtr stamp) : ArithmeticalExpressionNode(std::move(stamp)){};

SubExpressionNode::SubExpressionNode(SubExpressionNode* other) : ArithmeticalExpressionNode(other) {}

ExpressionNodePtr SubExpressionNode::create(const ExpressionNodePtr left, const ExpressionNodePtr right) {
    auto subNode = std::make_shared<SubExpressionNode>(left->getStamp());
    subNode->setChildren(left, right);
    return subNode;
}

bool SubExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<SubExpressionNode>()) {
        auto otherSubNode = rhs->as<SubExpressionNode>();
        return getLeft()->equal(otherSubNode->getLeft()) && getRight()->equal(otherSubNode->getRight());
    }
    return false;
}
const std::string SubExpressionNode::toString() const {
    return "SubNode(" + stamp->toString() + ")";
}
ExpressionNodePtr SubExpressionNode::copy() {
    return std::make_shared<SubExpressionNode>(SubExpressionNode(this));
}

}// namespace NES