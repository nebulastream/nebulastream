
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
namespace NES {
GreaterExpressionNode::GreaterExpressionNode() :
    LogicalBinaryExpressionNode() {};
ExpressionNodePtr GreaterExpressionNode::create(const ExpressionNodePtr left,
                                                 const ExpressionNodePtr right) {
    auto greater = std::make_shared<GreaterExpressionNode>();
    greater->setChildren(left, right);
    return greater;
}

bool GreaterExpressionNode::equal(const NodePtr rhs) const {
    if(this->isIdentical(rhs)){
        return true;
    }
    return rhs->instanceOf<GreaterExpressionNode>();
}

const std::string GreaterExpressionNode::toString() const {
    return "GreaterNode("+stamp->toString()+")";
}

}