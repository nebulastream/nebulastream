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
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <utility>
namespace NES {
FieldAccessExpressionNode::FieldAccessExpressionNode(DataTypePtr stamp, std::string fieldName)
    : ExpressionNode(std::move(stamp)), fieldName(std::move(fieldName)){};

FieldAccessExpressionNode::FieldAccessExpressionNode(FieldAccessExpressionNode* other)
    : ExpressionNode(other), fieldName(other->getFieldName()){};

ExpressionNodePtr FieldAccessExpressionNode::create(DataTypePtr stamp, std::string fieldName) {
    return std::make_shared<FieldAccessExpressionNode>(FieldAccessExpressionNode(std::move(stamp), std::move(fieldName)));
}

ExpressionNodePtr FieldAccessExpressionNode::create(std::string fieldName) {
    return create(DataTypeFactory::createUndefined(), std::move(fieldName));
}

bool FieldAccessExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<FieldAccessExpressionNode>()) {
        auto otherFieldRead = rhs->as<FieldAccessExpressionNode>();
        return otherFieldRead->fieldName == fieldName && otherFieldRead->stamp->isEquals(stamp);
    }
    return false;
}

std::string FieldAccessExpressionNode::getFieldName() { return fieldName; }

void FieldAccessExpressionNode::updateFieldName(std::string fieldName) { this->fieldName = std::move(fieldName); }

std::string FieldAccessExpressionNode::toString() const {
    return "FieldAccessNode(" + fieldName + "[" + stamp->toString() + "])";
}

void FieldAccessExpressionNode::inferStamp(SchemaPtr schema) {
    // check if the access field is defined in the schema.
    auto existingField = schema->hasFieldName(fieldName);
    if (existingField) {
        fieldName = existingField->getName();
        stamp = existingField->getDataType();
        return;
    }
    throw std::logic_error("FieldAccessExpression: the field " + fieldName + " is not defined in the  schema "
                           + schema->toString());
}

ExpressionNodePtr FieldAccessExpressionNode::copy() {
    return std::make_shared<FieldAccessExpressionNode>(FieldAccessExpressionNode(this));
}
}// namespace NES