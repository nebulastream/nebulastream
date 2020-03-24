
#include <Nodes/Expressions/UnaryExpressions/NegateExpressionNode.hpp>

namespace NES {

NegateExpressionNode::NegateExpressionNode() : LogicalUnaryExpressionNode() {}

bool NegateExpressionNode::equal(const NodePtr rhs) const {
    return true;
}

const std::string NegateExpressionNode::toString() const {
    return "NegateNode()";
}

ExpressionNodePtr NegateExpressionNode::create(const ExpressionNodePtr child) {
    auto equals = std::make_shared<NegateExpressionNode>();
    equals->setChild(child);
    return equals;
}

}