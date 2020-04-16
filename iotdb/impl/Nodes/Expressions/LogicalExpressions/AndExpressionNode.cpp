
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
namespace NES {
AndExpressionNode::AndExpressionNode() : LogicalBinaryExpressionNode() {};
ExpressionNodePtr AndExpressionNode::create(const ExpressionNodePtr left, const ExpressionNodePtr right) {
    auto andNode = std::make_shared<AndExpressionNode>();
    andNode->setChildren(left, right);
    return andNode;
}

bool AndExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<AndExpressionNode>()) {
        auto otherAndNode = rhs->as<AndExpressionNode>();
        return getLeft()->equal(otherAndNode->getLeft()) &&
            getRight()->equal(otherAndNode->getRight());
    }
    return false;
}
const std::string AndExpressionNode::toString() const {
    return "AndNode()";
}

}