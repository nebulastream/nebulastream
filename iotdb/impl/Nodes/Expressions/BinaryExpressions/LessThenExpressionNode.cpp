
#include <Nodes/Expressions/BinaryExpressions/LessThenExpressionNode.hpp>
namespace NES {
LessThenExpressionNode::LessThenExpressionNode() :
    LogicalBinaryExpressionNode() {};
ExpressionNodePtr LessThenExpressionNode::create(const ExpressionNodePtr left,
                                                 const ExpressionNodePtr right) {
    auto lessThen = std::make_shared<LessThenExpressionNode>();
    lessThen->setChildren(left, right);
    return lessThen;
}

bool LessThenExpressionNode::equal(const NodePtr rhs) const {
    return false;
}
const std::string LessThenExpressionNode::toString() const {
    return "LessThenNode()";
}

}