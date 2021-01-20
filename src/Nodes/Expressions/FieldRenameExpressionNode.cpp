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
#include <Exceptions/TypeInferenceException.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldRenameExpressionNode.hpp>
#include <utility>

namespace NES {
FieldRenameExpressionNode::FieldRenameExpressionNode(ExpressionNodePtr expression, std::string newFieldName)
    : FieldAccessExpressionNode(expression->getStamp(), expression->as<FieldAccessExpressionNode>()->getFieldName()),
      newFieldName(newFieldName){};

FieldRenameExpressionNode::FieldRenameExpressionNode(FieldRenameExpressionNode* other)
    : FieldRenameExpressionNode(other->expression, other->getFieldName()){};

ExpressionNodePtr FieldRenameExpressionNode::create(ExpressionNodePtr expression, std::string newFieldName) {
    return std::make_shared<FieldRenameExpressionNode>(FieldRenameExpressionNode(expression, newFieldName));
}

bool FieldRenameExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<FieldRenameExpressionNode>()) {
        auto otherFieldRead = rhs->as<FieldRenameExpressionNode>();
        return otherFieldRead->fieldName == fieldName && otherFieldRead->stamp->isEquals(stamp);
    }
    return false;
}

const std::string FieldRenameExpressionNode::getNewFieldName() { return newFieldName; }

const std::string FieldRenameExpressionNode::toString() const {
    return "FieldRenameExpression(" + fieldName + ": " + stamp->toString() + ")";
}

void FieldRenameExpressionNode::inferStamp(SchemaPtr schema) {



    //Detect if user has provided fully qualified name
    if (fieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos) {
        fieldName = schema->getQualifierName() + fieldName;
    }

    //Detect if user has provided fully qualified nam
    if (newFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos) {
        newFieldName = schema->getQualifierName() + newFieldName;
    }

    if (fieldName == newFieldName) {
        NES_WARNING("FieldRenameExpressionNode: Both existing and new fields are same: existing: " + fieldName
                    + " new field name: " + newFieldName);
    } else if (schema->hasFullyQualifiedFieldName(newFieldName)) {
        NES_ERROR("FieldRenameExpressionNode: The new field name" + newFieldName + " already exists in the input schema "
                  + schema->toString() + ". Can't use the name of existing field.");
        throw TypeInferenceException("FieldRenameExpressionNode: The new field name" + newFieldName
                                     + " already exists in the input schema " + schema->toString());
    }

    // check if the access field is defined in the schema.
    if (!schema->hasFullyQualifiedFieldName(fieldName)) {
        NES_ERROR("FieldAccessExpression: the old field " + fieldName + " is not defined in the Input schema "
                  + schema->toString());
        throw TypeInferenceException("FieldAccessExpression: the old field " + fieldName + " is not defined in the Input schema "
                                     + schema->toString());
    }
    // assign the stamp of this field access with the type of this field.
    auto field = schema->get(fieldName);
    stamp = field->getDataType();
}

ExpressionNodePtr FieldRenameExpressionNode::copy() {
    return std::make_shared<FieldRenameExpressionNode>(FieldRenameExpressionNode(this));
}

}// namespace NES