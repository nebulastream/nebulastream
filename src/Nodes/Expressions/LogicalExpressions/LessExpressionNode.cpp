
#include <Common/DataTypes/DataType.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
namespace NES {
LessExpressionNode::LessExpressionNode() : LogicalBinaryExpressionNode(){};

LessExpressionNode::LessExpressionNode(LessExpressionNode* other) : LogicalBinaryExpressionNode(other) {}

ExpressionNodePtr LessExpressionNode::create(const ExpressionNodePtr left,
                                             const ExpressionNodePtr right) {
    auto lessThen = std::make_shared<LessExpressionNode>();
    lessThen->setChildren(left, right);
    return lessThen;
}

bool LessExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<LessExpressionNode>()) {
        auto other = rhs->as<LessExpressionNode>();
        return this->getLeft()->equal(other->getLeft()) && this->getRight()->equal(other->getRight());
    }
    return false;
}

const std::string LessExpressionNode::toString() const {
    return "LessNode(" + stamp->toString() + ")";
}
ExpressionNodePtr LessExpressionNode::copy() {
    return std::make_shared<LessExpressionNode>(LessExpressionNode(this));
}

}// namespace NES