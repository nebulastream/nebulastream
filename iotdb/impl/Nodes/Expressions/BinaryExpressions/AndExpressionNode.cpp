
#include <Nodes/Expressions/BinaryExpressions/AndExpressionNode.hpp>
namespace NES {
AndExpressionNode::AndExpressionNode() :
    LogicalBinaryExpressionNode() {};
ExpressionNodePtr AndExpressionNode::create(const ExpressionNodePtr left, const ExpressionNodePtr right) {
    auto andNode = std::make_shared<AndExpressionNode>();
    andNode->setChildren(left, right);
    return andNode;
}

bool AndExpressionNode::equal(const NodePtr rhs) const {
    return false;
}
const std::string AndExpressionNode::toString() const {
    return "AndNode()";
}

}