#include <Common/DataTypes/DataType.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <utility>
namespace NES {

DivExpressionNode::DivExpressionNode(DataTypePtr stamp) : ArithmeticalExpressionNode(std::move(stamp)){};

DivExpressionNode::DivExpressionNode(DivExpressionNode* other) : ArithmeticalExpressionNode(other) {}

ExpressionNodePtr DivExpressionNode::create(const ExpressionNodePtr left, const ExpressionNodePtr right) {
    auto divNode = std::make_shared<DivExpressionNode>(left->getStamp());
    divNode->setChildren(left, right);
    return divNode;
}

bool DivExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<DivExpressionNode>()) {
        auto otherDivNode = rhs->as<DivExpressionNode>();
        return getLeft()->equal(otherDivNode->getLeft()) && getRight()->equal(otherDivNode->getRight());
    }
    return false;
}
const std::string DivExpressionNode::toString() const {
    return "DivNode(" + stamp->toString() + ")";
}
ExpressionNodePtr DivExpressionNode::copy() {
    return std::make_shared<DivExpressionNode>(DivExpressionNode(this));
}


}// namespace NES