
#include <Nodes/Expressions/BinaryExpressions/GreaterThenExpressionNode.hpp>
namespace NES {
GreaterThenExpressionNode::GreaterThenExpressionNode() :
    LogicalBinaryExpressionNode() {};
ExpressionNodePtr GreaterThenExpressionNode::create(const ExpressionNodePtr left,
                                                 const ExpressionNodePtr right) {
    auto greaterThen = std::make_shared<GreaterThenExpressionNode>();
    greaterThen->setChildren(left, right);
    return greaterThen;
}

bool GreaterThenExpressionNode::equal(const NodePtr rhs) const {
    return false;
}
const std::string GreaterThenExpressionNode::toString() const {
    return "GreaterThenNode()";
}

}