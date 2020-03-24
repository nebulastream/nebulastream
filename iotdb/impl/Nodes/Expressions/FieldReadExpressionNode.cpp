
#include <Nodes/Expressions/FieldReadExpressionNode.hpp>
#include <utility>
namespace NES {
FieldReadExpressionNode::FieldReadExpressionNode(DataTypePtr stamp, std::string fieldName)
    : ExpressionNode(std::move(stamp)), fieldName(std::move(fieldName)) {};

FieldReadExpressionNode::FieldReadExpressionNode(std::string fieldName)
    : FieldReadExpressionNode(nullptr, std::move(fieldName)) {};

ExpressionNodePtr FieldReadExpressionNode::create(DataTypePtr stamp, std::string fieldName){
    return std::make_shared<FieldReadExpressionNode>(stamp, fieldName);
}

bool FieldReadExpressionNode::equal(const NodePtr rhs) const {
    if(rhs->instanceOf<FieldReadExpressionNode>()){
        auto otherFieldRead = rhs->as<FieldReadExpressionNode>();
        return otherFieldRead->fieldName == fieldName && otherFieldRead->stamp == stamp;
    }
    return false;
}

const std::string FieldReadExpressionNode::toString() const {
    return "FieldReadNode("+fieldName+")";
}


}