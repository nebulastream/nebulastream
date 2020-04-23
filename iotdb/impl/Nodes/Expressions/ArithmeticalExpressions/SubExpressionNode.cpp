#include <Nodes/Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
namespace NES {
SubExpressionNode::SubExpressionNode(DataTypePtr stamp) : ArithmeticalExpressionNode(stamp) {};
ExpressionNodePtr SubExpressionNode::create(const ExpressionNodePtr left, const ExpressionNodePtr right) {
    auto subNode = std::make_shared<SubExpressionNode>(left->getStamp());
    subNode->setChildren(left, right);
    return subNode;
}

bool SubExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<SubExpressionNode>()) {
        auto otherSubNode = rhs->as<SubExpressionNode>();
        return getLeft()->equal(otherSubNode->getLeft()) &&
            getRight()->equal(otherSubNode->getRight());
    }
    return false;
}
const std::string SubExpressionNode::toString() const {
    return "SubNode("+stamp->toString()+")";
}

}