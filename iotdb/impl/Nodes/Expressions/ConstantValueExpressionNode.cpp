
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <QueryCompiler/DataTypes/ValueType.hpp>
namespace NES {
ConstantValueExpressionNode::ConstantValueExpressionNode(const ValueTypePtr constantValue) :
    ExpressionNode(constantValue->getType()),
    constantValue(constantValue) {};


bool ConstantValueExpressionNode::equal(const NodePtr rhs) const {
    return false;
}

const std::string ConstantValueExpressionNode::toString() const {
    return "ConstantValueNode()";
}

ExpressionNodePtr ConstantValueExpressionNode::create(const ValueTypePtr constantValue){
    return std::make_shared<ConstantValueExpressionNode>(constantValue);
}

}