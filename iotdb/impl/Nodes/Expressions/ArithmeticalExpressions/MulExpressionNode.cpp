#include <Nodes/Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
namespace NES {
MulExpressionNode::MulExpressionNode(DataTypePtr stamp) : ArithmeticalExpressionNode(stamp) {};
ExpressionNodePtr MulExpressionNode::create(const ExpressionNodePtr left, const ExpressionNodePtr right) {
    auto mulNode = std::make_shared<MulExpressionNode>(left->getStamp());
    mulNode->setChildren(left, right);
    return mulNode;
}

bool MulExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<MulExpressionNode>()) {
        auto otherMulNode = rhs->as<MulExpressionNode>();
        return getLeft()->equal(otherMulNode->getLeft()) &&
            getRight()->equal(otherMulNode->getRight());
    }
    return false;
}
const std::string MulExpressionNode::toString() const {
    return "MulNode("+stamp->toString()+")";
}

}