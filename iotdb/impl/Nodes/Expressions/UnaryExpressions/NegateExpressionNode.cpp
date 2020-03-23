
#include <Nodes/Expressions/UnaryExpressions/NegateExpressionNode.hpp>

namespace NES {

NegateExpressionNode::NegateExpressionNode(ExpressionNodePtr child) :
    LogicalUnaryExpressionNode(child) {
}
bool NegateExpressionNode::equal(const NodePtr rhs) const {
    return false;
}
const std::string NegateExpressionNode::toString() const {
    return std::__cxx11::string();
}

std::shared_ptr<NegateExpressionNode> createNegateNode(const ExpressionNodePtr child) {
    return std::make_shared<NegateExpressionNode>(child);
};
}