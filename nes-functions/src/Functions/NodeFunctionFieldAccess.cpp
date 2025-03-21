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
#include <memory>
#include <string>
#include <utility>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Nodes/Node.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

#include <Identifiers/Identifier.hpp>
namespace NES
{
NodeFunctionFieldAccess::NodeFunctionFieldAccess(DataType stamp, IdentifierList  fieldName)
    : NodeFunction(std::move(stamp), "FieldAccess"), fieldName(std::move(fieldName)) {};

NodeFunctionFieldAccess::NodeFunctionFieldAccess(const NodeFunctionFieldAccess* other) : NodeFunction(other), fieldName(other->getFieldName()) {};

std::shared_ptr<NodeFunction> NodeFunctionFieldAccess::create(DataType stamp, const IdentifierList& fieldName)
{
    return std::make_shared<NodeFunctionFieldAccess>(NodeFunctionFieldAccess(std::move(stamp), fieldName));
}

std::shared_ptr<NodeFunction> NodeFunctionFieldAccess::create(const IdentifierList& fieldName)
{
    return create(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED), fieldName);
}

bool NodeFunctionFieldAccess::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionFieldAccess>(rhs))
    {
        auto otherFieldRead = NES::Util::as<NodeFunctionFieldAccess>(rhs);
        bool fieldNamesMatch = otherFieldRead->fieldName == fieldName;
        const bool stampsMatch = otherFieldRead->stamp == stamp;
        return fieldNamesMatch and stampsMatch;
    }
    return false;
}

IdentifierList NodeFunctionFieldAccess::getFieldName() const
{
    return fieldName;
}

void NodeFunctionFieldAccess::updateFieldName(const IdentifierList& fieldName)
{
    this->fieldName = fieldName;
}

std::string NodeFunctionFieldAccess::toString() const
{
    return fmt::format("NodeFunctionFieldAccess( {} [ {} ])", fieldName, stamp);
}

void NodeFunctionFieldAccess::inferStamp(const Schema& schema)
{
    /// check if the access field is defined in the schema.
    if (const auto existingField = schema.getFieldByName(fieldName))
    {
        fieldName = existingField.value().name;
        stamp = existingField.value().dataType;
        return;
    }
    throw QueryInvalid("FieldAccessFunction: the field {} is not defined in the schema {}", fieldName, schema);
}

std::shared_ptr<NodeFunction> NodeFunctionFieldAccess::deepCopy()
{
    return std::make_shared<NodeFunctionFieldAccess>(*this);
}

bool NodeFunctionFieldAccess::validateBeforeLowering() const
{
    return children.empty();
}
}
