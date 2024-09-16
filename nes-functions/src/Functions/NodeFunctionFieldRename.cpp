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
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldRename.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{
NodeFunctionFieldRename::NodeFunctionFieldRename(const NodeFunctionFieldAccessPtr& originalField, std::string newFieldName)
    : FunctionNode(originalField->getStamp(), "FieldRename"), originalField(originalField), newFieldName(std::move(newFieldName)) {};

NodeFunctionFieldRename::NodeFunctionFieldRename(const NodeFunctionFieldRenamePtr other)
    : NodeFunctionFieldRename(other->getOriginalField(), other->getNewFieldName()) {};

NodeFunctionPtr NodeFunctionFieldRename::create(NodeFunctionFieldAccessPtr originalField, std::string newFieldName)
{
    return std::make_shared<NodeFunctionFieldRename>(NodeFunctionFieldRename(originalField, std::move(newFieldName)));
}

bool NodeFunctionFieldRename::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<NodeFunctionFieldRename>())
    {
        auto otherFieldRead = rhs->as<NodeFunctionFieldRename>();
        return otherFieldRead->getOriginalField()->equal(getOriginalField()) && this->newFieldName == otherFieldRead->getNewFieldName();
    }
    return false;
}

NodeFunctionFieldAccessPtr NodeFunctionFieldRename::getOriginalField() const
{
    return this->originalField;
}

std::string NodeFunctionFieldRename::getNewFieldName() const
{
    return newFieldName;
}

std::string NodeFunctionFieldRename::toString() const
{
    auto node = getOriginalField();
    return "FieldRenameFunction(" + getOriginalField()->toString() + " => " + newFieldName + " : " + stamp->toString() + ")";
}

void NodeFunctionFieldRename::inferStamp(SchemaPtr schema)
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
                "NodeFunctionFieldRename: Original field with name {} does not exists in the schema {}", fieldName, schema->toString());
            throw InvalidFieldException("Original field with name " + fieldName + " does not exists in the schema " + schema->toString());
        }
        newFieldName = fieldName.substr(0, fieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1) + newFieldName;
    }

    if (fieldName == newFieldName)
    {
        NES_WARNING(
            "NodeFunctionFieldRename: Both existing and new fields are same: existing: {} new field name: {}", fieldName, newFieldName);
    }
    else
    {
        auto newFieldAttribute = schema->getField(newFieldName);
        if (newFieldAttribute)
        {
            NES_ERROR(
                "NodeFunctionFieldRename: The new field name {} already exists in the input schema {}. "
                "Can't use the name of an existing field.",
                schema->toString(),
                newFieldName);
            throw InvalidFieldException("New field with name " + newFieldName + " already exists in the schema " + schema->toString());
        }
    }
    /// assign the stamp of this field access with the type of this field.
    stamp = fieldAttribute->getDataType();
}

NodeFunctionPtr NodeFunctionFieldRename::deepCopy()
{
    return NodeFunctionFieldRename::create(originalField->deepCopy()->as<NodeFunctionFieldAccess>(), newFieldName);
}

bool NodeFunctionFieldRename::validateBeforeLowering() const
{
    ///NodeFunction Currently, we do not have any validation for FieldRename before lowering
    return true;
}

} /// namespace NES
