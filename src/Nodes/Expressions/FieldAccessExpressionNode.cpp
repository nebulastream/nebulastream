/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <API/Schema.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <utility>
namespace NES {
FieldAccessExpressionNode::FieldAccessExpressionNode(DataTypePtr stamp, std::string fieldName)
    : ExpressionNode(std::move(stamp)), fieldName(std::move(fieldName)){};

FieldAccessExpressionNode::FieldAccessExpressionNode(FieldAccessExpressionNode* other) : ExpressionNode(other), fieldName(other->getFieldName()){};

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