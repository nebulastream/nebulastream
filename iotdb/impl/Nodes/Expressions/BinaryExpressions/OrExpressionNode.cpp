
#include <Nodes/Expressions/BinaryExpressions/OrExpressionNode.hpp>
namespace NES {
OrExpressionNode::OrExpressionNode() : LogicalBinaryExpressionNode() {};
ExpressionNodePtr OrExpressionNode::create(const ExpressionNodePtr left, const ExpressionNodePtr right) {
    auto orNode = std::make_shared<OrExpressionNode>();
    orNode->setChildren(left, right);
    return orNode;
}

bool OrExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<OrExpressionNode>()) {
        auto otherAndNode = rhs->as<OrExpressionNode>();
        return getLeft()->equal(otherAndNode->getLeft()) &&
            getRight()->equal(otherAndNode->getRight());
    }
    return false;
}
const std::string OrExpressionNode::toString() const {
    return "OrNode()";
}

}