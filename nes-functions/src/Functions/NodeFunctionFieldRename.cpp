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
<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionFieldRename.cpp
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldRename.hpp>
#include <Util/Common.hpp>
========
#include <Functions/FieldAccessFunctionNode.hpp>
#include <Functions/FieldRenameFunctionNode.hpp>
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/FieldRenameFunctionNode.cpp
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>


namespace NES
{
<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionFieldRename.cpp
NodeFunctionFieldRename::NodeFunctionFieldRename(const NodeFunctionFieldAccessPtr& originalField, std::string newFieldName)
    : NodeFunction(originalField->getStamp(), "FieldRename"), originalField(originalField), newFieldName(std::move(newFieldName)) {};

NodeFunctionFieldRename::NodeFunctionFieldRename(const NodeFunctionFieldRenamePtr other)
    : NodeFunctionFieldRename(other->getOriginalField(), other->getNewFieldName()) {};

NodeFunctionPtr NodeFunctionFieldRename::create(NodeFunctionFieldAccessPtr originalField, std::string newFieldName)
{
    return std::make_shared<NodeFunctionFieldRename>(NodeFunctionFieldRename(originalField, std::move(newFieldName)));
}

bool NodeFunctionFieldRename::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionFieldRename>(rhs))
    {
        auto otherFieldRead = NES::Util::as<NodeFunctionFieldRename>(rhs);
========
FieldRenameFunctionNode::FieldRenameFunctionNode(const FieldAccessFunctionNodePtr& originalField, std::string newFieldName)
    : FunctionNode(originalField->getStamp()), originalField(originalField), newFieldName(std::move(newFieldName)) {};

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
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/FieldRenameFunctionNode.cpp
        return otherFieldRead->getOriginalField()->equal(getOriginalField()) && this->newFieldName == otherFieldRead->getNewFieldName();
    }
    return false;
}

<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionFieldRename.cpp
NodeFunctionFieldAccessPtr NodeFunctionFieldRename::getOriginalField() const
========
FieldAccessFunctionNodePtr FieldRenameFunctionNode::getOriginalField() const
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/FieldRenameFunctionNode.cpp
{
    return this->originalField;
}

<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionFieldRename.cpp
std::string NodeFunctionFieldRename::getNewFieldName() const
========
std::string FieldRenameFunctionNode::getNewFieldName() const
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/FieldRenameFunctionNode.cpp
{
    return newFieldName;
}

<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionFieldRename.cpp
std::string NodeFunctionFieldRename::toString() const
========
std::string FieldRenameFunctionNode::toString() const
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/FieldRenameFunctionNode.cpp
{
    auto node = getOriginalField();
    return "FieldRenameFunction(" + getOriginalField()->toString() + " => " + newFieldName + " : " + stamp->toString() + ")";
}

<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionFieldRename.cpp
void NodeFunctionFieldRename::inferStamp(SchemaPtr schema)
========
void FieldRenameFunctionNode::inferStamp(SchemaPtr schema)
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/FieldRenameFunctionNode.cpp
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
<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionFieldRename.cpp
            throw InvalidFieldException(fmt::format(
                "NodeFunctionFieldRename: Original field with name {} does not exists in the schema {}", fieldName, schema->toString()));
========
            NES_ERROR(
                "FieldRenameFunctionNode: Original field with name {} does not exists in the schema {}", fieldName, schema->toString());
            throw InvalidFieldException("Original field with name " + fieldName + " does not exists in the schema " + schema->toString());
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/FieldRenameFunctionNode.cpp
        }
        newFieldName = fieldName.substr(0, fieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1) + newFieldName;
    }

    if (fieldName == newFieldName)
    {
        NES_WARNING(
<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionFieldRename.cpp
            "NodeFunctionFieldRename: Both existing and new fields are same: existing: {} new field name: {}", fieldName, newFieldName);
========
            "FieldRenameFunctionNode: Both existing and new fields are same: existing: {} new field name: {}", fieldName, newFieldName);
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/FieldRenameFunctionNode.cpp
    }
    else
    {
        auto newFieldAttribute = schema->getField(newFieldName);
        if (newFieldAttribute)
        {
            NES_ERROR(
<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionFieldRename.cpp
                "NodeFunctionFieldRename: The new field name {} already exists in the input schema {}. "
========
                "FieldRenameFunctionNode: The new field name {} already exists in the input schema {}. "
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/FieldRenameFunctionNode.cpp
                "Can't use the name of an existing field.",
                schema->toString(),
                newFieldName);
            throw InvalidFieldException("New field with name " + newFieldName + " already exists in the schema " + schema->toString());
        }
    }
    /// assign the stamp of this field access with the type of this field.
    stamp = fieldAttribute->getDataType();
}

<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionFieldRename.cpp
NodeFunctionPtr NodeFunctionFieldRename::deepCopy()
{
    return NodeFunctionFieldRename::create(Util::as<NodeFunctionFieldAccess>(originalField->deepCopy()), newFieldName);
========
FunctionNodePtr FieldRenameFunctionNode::copy()
{
    return FieldRenameFunctionNode::create(originalField->copy()->as<FieldAccessFunctionNode>(), newFieldName);
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/FieldRenameFunctionNode.cpp
}

bool NodeFunctionFieldRename::validateBeforeLowering() const
{
    ///NodeFunction Currently, we do not have any validation for FieldRename before lowering
    return true;
}

}
