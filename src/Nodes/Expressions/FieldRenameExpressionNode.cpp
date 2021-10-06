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
#include <Exceptions/InvalidFieldException.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldRenameExpressionNode.hpp>
#include <utility>

namespace NES {
FieldRenameExpressionNode::FieldRenameExpressionNode(const FieldAccessExpressionNodePtr& originalField, std::string newFieldName)
    : ExpressionNode(originalField->getStamp()), originalField(originalField), newFieldName(std::move(newFieldName)){};

FieldRenameExpressionNode::FieldRenameExpressionNode(FieldRenameExpressionNode* other)
    : FieldRenameExpressionNode(other->getOriginalField(), other->getNewFieldName()){};

ExpressionNodePtr FieldRenameExpressionNode::create(FieldAccessExpressionNodePtr originalField, std::string newFieldName) {
    return std::make_shared<FieldRenameExpressionNode>(
        FieldRenameExpressionNode(std::move(originalField), std::move(newFieldName)));
}

bool FieldRenameExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<FieldRenameExpressionNode>()) {
        auto otherFieldRead = rhs->as<FieldRenameExpressionNode>();
        return otherFieldRead->getOriginalField()->equal(getOriginalField())
            && this->newFieldName == otherFieldRead->getNewFieldName();
    }
    return false;
}

FieldAccessExpressionNodePtr FieldRenameExpressionNode::getOriginalField() const { return this->originalField; }

std::string FieldRenameExpressionNode::getNewFieldName() { return newFieldName; }

std::string FieldRenameExpressionNode::toString() const {
    auto node = getOriginalField();
    return "FieldRenameExpression(" + getOriginalField()->toString() + " => " + newFieldName + " : " + stamp->toString() + ")";
}

void FieldRenameExpressionNode::inferStamp(SchemaPtr schema) {

    auto originalFieldName = getOriginalField();
    originalFieldName->inferStamp(schema);
    auto fieldName = originalFieldName->getFieldName();
    auto fieldAttribute = schema->hasFieldName(fieldName);
    //Detect if user has added attribute name separator
    if (newFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos) {
        if (!fieldAttribute) {
            NES_ERROR("FieldRenameExpressionNode: Original field with name " << fieldName << " does not exists in the schema "
                                                                             << schema->toString());
            throw InvalidFieldException("Original field with name " + fieldName + " does not exists in the schema "
                                        + schema->toString());
        }
        newFieldName = fieldName.substr(0, fieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1) + newFieldName;
    }

    if (fieldName == newFieldName) {
        NES_WARNING("FieldRenameExpressionNode: Both existing and new fields are same: existing: " + fieldName
                    + " new field name: " + newFieldName);
    } else {
        auto newFieldAttribute = schema->hasFieldName(newFieldName);
        if (newFieldAttribute) {
            NES_ERROR("FieldRenameExpressionNode: The new field name" + newFieldName + " already exists in the input schema "
                      + schema->toString() + ". Can't use the name of an existing field.");
            throw InvalidFieldException("New field with name " + newFieldName + " already exists in the schema "
                                        + schema->toString());
        }
    }
    // assign the stamp of this field access with the type of this field.
    stamp = fieldAttribute->getDataType();
}

ExpressionNodePtr FieldRenameExpressionNode::copy() {
    return std::make_shared<FieldRenameExpressionNode>(FieldRenameExpressionNode(this));
}

}// namespace NES