
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <utility>
namespace NES {
FieldAccessExpressionNode::FieldAccessExpressionNode(DataTypePtr stamp, std::string fieldName)
    : ExpressionNode(std::move(stamp)), fieldName(std::move(fieldName)) {};

FieldAccessExpressionNode::FieldAccessExpressionNode(std::string fieldName)
    : FieldAccessExpressionNode(nullptr, std::move(fieldName)) {};

ExpressionNodePtr FieldAccessExpressionNode::create(DataTypePtr stamp, std::string fieldName) {
    return std::make_shared<FieldAccessExpressionNode>(stamp, fieldName);
}

ExpressionNodePtr FieldAccessExpressionNode::create(std::string fieldName) {
    return create(nullptr, fieldName);
}

bool FieldAccessExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<FieldAccessExpressionNode>()) {
        auto otherFieldRead = rhs->as<FieldAccessExpressionNode>();
        return otherFieldRead->fieldName == fieldName && otherFieldRead->stamp == stamp;
    }
    return false;
}

const std::string FieldAccessExpressionNode::getFieldName() {
    return fieldName;
}

const std::string FieldAccessExpressionNode::toString() const {
    return "FieldAccessNode(" + fieldName + ")";
}

}