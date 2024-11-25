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
#include <format>
#include <utility>

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
namespace NES
{
NodeFunctionFieldAccess::NodeFunctionFieldAccess(DataTypePtr stamp, std::string fieldName)
    : NodeFunction(std::move(stamp), "FieldAccess"), fieldName(std::move(fieldName)) {};

NodeFunctionFieldAccess::NodeFunctionFieldAccess(NodeFunctionFieldAccess* other) : NodeFunction(other), fieldName(other->getFieldName()) {};

NodeFunctionPtr NodeFunctionFieldAccess::create(DataTypePtr stamp, std::string fieldName)
{
    return std::make_shared<NodeFunctionFieldAccess>(NodeFunctionFieldAccess(std::move(stamp), std::move(fieldName)));
}

NodeFunctionPtr NodeFunctionFieldAccess::create(std::string fieldName)
{
    return create(DataTypeFactory::createUndefined(), std::move(fieldName));
}

bool NodeFunctionFieldAccess::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionFieldAccess>(rhs))
    {
        auto otherFieldRead = NES::Util::as<NodeFunctionFieldAccess>(rhs);
        bool fieldNamesMatch = otherFieldRead->fieldName == fieldName;
        bool const stampsMatch = *otherFieldRead->stamp == *stamp;
        return fieldNamesMatch and stampsMatch;
    }
    return false;
}

std::string NodeFunctionFieldAccess::getFieldName() const
{
    return fieldName;
}

void NodeFunctionFieldAccess::updateFieldName(std::string fieldName)
{
    this->fieldName = std::move(fieldName);
}

std::string NodeFunctionFieldAccess::toString() const
{
    return std::format("NodeFunctionFieldAccess( {} [ {} ])", fieldName, stamp->toString());
}

void NodeFunctionFieldAccess::inferStamp(SchemaPtr schema)
{
    /// check if the access field is defined in the schema.
    if (const auto existingField = schema->getFieldByName(fieldName))
    {
        fieldName = existingField.value()->getName();
        stamp = existingField.value()->getDataType();
        return;
    }
    throw QueryInvalid("FieldAccessFunction: the field {} is not defined in the schema {}", fieldName, schema->toString());
}

NodeFunctionPtr NodeFunctionFieldAccess::deepCopy()
{
    return std::make_shared<NodeFunctionFieldAccess>(*this);
}

bool NodeFunctionFieldAccess::validateBeforeLowering() const
{
    return children.empty();
}
}
