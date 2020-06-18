
#include <DataTypes/DataType.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
namespace NES {
LessExpressionNode::LessExpressionNode() : LogicalBinaryExpressionNode(){};
ExpressionNodePtr LessExpressionNode::create(const ExpressionNodePtr left,
                                             const ExpressionNodePtr right) {
    auto lessThen = std::make_shared<LessExpressionNode>();
    lessThen->setChildren(left, right);
    return lessThen;
}

bool LessExpressionNode::equal(const NodePtr rhs) const {
    if (this->isIdentical(rhs)) {
        return true;
    }
    return rhs->instanceOf<LessExpressionNode>();
}

const std::string LessExpressionNode::toString() const {
    return "LessNode(" + stamp->toString() + ")";
}

}// namespace NES