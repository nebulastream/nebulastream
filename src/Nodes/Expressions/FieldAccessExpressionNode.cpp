
#include <API/Schema.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <utility>
namespace NES {
FieldAccessExpressionNode::FieldAccessExpressionNode(DataTypePtr stamp, std::string fieldName)
    : ExpressionNode(std::move(stamp)), fieldName(std::move(fieldName)){};

FieldAccessExpressionNode::FieldAccessExpressionNode(FieldAccessExpressionNode* other): ExpressionNode(other), fieldName(other->getFieldName()){};

ExpressionNodePtr FieldAccessExpressionNode::create(DataTypePtr stamp, std::string fieldName) {
    return std::make_shared<FieldAccessExpressionNode>(FieldAccessExpressionNode(stamp, fieldName));
}

ExpressionNodePtr FieldAccessExpressionNode::create(std::string fieldName) {
    return create(DataTypeFactory::createUndefined(), fieldName);
}

bool FieldAccessExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<FieldAccessExpressionNode>()) {
        auto otherFieldRead = rhs->as<FieldAccessExpressionNode>();
        return otherFieldRead->fieldName == fieldName && otherFieldRead->stamp->isEquals(stamp);
    }
    return false;
}

const std::string FieldAccessExpressionNode::getFieldName() {
    return fieldName;
}

const std::string FieldAccessExpressionNode::toString() const {
    return "FieldAccessNode(" + fieldName + ": " + stamp->toString() + ")";
}

void FieldAccessExpressionNode::inferStamp(SchemaPtr schema) {
    // check if the access field is defined in the schema.
    if (!schema->has(fieldName)) {
        NES_THROW_RUNTIME_ERROR(
            "FieldAccessExpression: the field " + fieldName + " is not defined in the  schema " + schema->toString());
    }
    // assign the stamp of this field access with the type of this field.
    auto field = schema->get(fieldName);
    stamp = field->getDataType();
}
void FieldAccessExpressionNode::setFieldName(std::string name) {
    this->fieldName = name;
}
ExpressionNodePtr FieldAccessExpressionNode::copy() {
    return std::make_shared<FieldAccessExpressionNode>(FieldAccessExpressionNode(this));
}

}// namespace NES