
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
namespace NES {
LessExpressionNode::LessExpressionNode() :
    LogicalBinaryExpressionNode() {};
ExpressionNodePtr LessExpressionNode::create(const ExpressionNodePtr left,
                                                 const ExpressionNodePtr right) {
    auto lessThen = std::make_shared<LessExpressionNode>();
    lessThen->setChildren(left, right);
    return lessThen;
}

bool LessExpressionNode::equal(const NodePtr rhs) const {
    return false;
}
const std::string LessExpressionNode::toString() const {
    return "LessNode("+stamp->toString()+")";
}

}