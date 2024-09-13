/*
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
#include <utility>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Exceptions/InvalidFieldException.hpp>
#include <Functions/FieldAccessFunctionNode.hpp>
#include <Functions/FieldRenameFunctionNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{
FieldRenameFunctionNode::FieldRenameFunctionNode(const FieldAccessFunctionNodePtr& originalField, std::string newFieldName)
    : FunctionNode(originalField->getStamp(), "FieldRename"), originalField(originalField), newFieldName(std::move(newFieldName)) {};

FieldRenameFunctionNode::FieldRenameFunctionNode(const FieldRenameFunctionNodePtr other)
    : FieldRenameFunctionNode(other->getOriginalField(), other->getNewFieldName()) {};

FunctionNodePtr FieldRenameFunctionNode::create(FieldAccessFunctionNodePtr originalField, std::string newFieldName)
{
    return std::make_shared<FieldRenameFunctionNode>(FieldRenameFunctionNode(originalField, std::move(newFieldName)));
}

bool FieldRenameFunctionNode::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<FieldRenameFunctionNode>())
    {
        auto otherFieldRead = rhs->as<FieldRenameFunctionNode>();
        return otherFieldRead->getOriginalField()->equal(getOriginalField()) && this->newFieldName == otherFieldRead->getNewFieldName();
    }
    return false;
}

FieldAccessFunctionNodePtr FieldRenameFunctionNode::getOriginalField() const
{
    return this->originalField;
}

std::string FieldRenameFunctionNode::getNewFieldName() const
{
    return newFieldName;
}

std::string FieldRenameFunctionNode::toString() const
{
    auto node = getOriginalField();
    return "FieldRenameFunction(" + getOriginalField()->toString() + " => " + newFieldName + " : " + stamp->toString() + ")";
}

void FieldRenameFunctionNode::inferStamp(SchemaPtr schema)
{
    auto originalFieldName = getOriginalField();
    originalFieldName->inferStamp(schema);
    auto fieldName = originalFieldName->getFieldName();
    auto fieldAttribute = schema->getField(fieldName);
    ///Detect if user has added attribute name separator
    if (newFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        if (!fieldAttribute)
        {
            NES_ERROR(
                "FieldRenameFunctionNode: Original field with name {} does not exists in the schema {}", fieldName, schema->toString());
            throw InvalidFieldException("Original field with name " + fieldName + " does not exists in the schema " + schema->toString());
        }
        newFieldName = fieldName.substr(0, fieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1) + newFieldName;
    }

    if (fieldName == newFieldName)
    {
        NES_WARNING(
            "FieldRenameFunctionNode: Both existing and new fields are same: existing: {} new field name: {}", fieldName, newFieldName);
    }
    else
    {
        auto newFieldAttribute = schema->getField(newFieldName);
        if (newFieldAttribute)
        {
            NES_ERROR(
                "FieldRenameFunctionNode: The new field name {} already exists in the input schema {}. "
                "Can't use the name of an existing field.",
                schema->toString(),
                newFieldName);
            throw InvalidFieldException("New field with name " + newFieldName + " already exists in the schema " + schema->toString());
        }
    }
    /// assign the stamp of this field access with the type of this field.
    stamp = fieldAttribute->getDataType();
}

FunctionNodePtr FieldRenameFunctionNode::deepCopy()
{
    return FieldRenameFunctionNode::create(originalField->deepCopy()->as<FieldAccessFunctionNode>(), newFieldName);
}

bool FieldRenameFunctionNode::validate() const
{
    NES_NOT_IMPLEMENTED();
}

} /// namespace NES
