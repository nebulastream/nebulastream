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

namespace NES
{
NodeFunctionFieldAccess::NodeFunctionFieldAccess(DataType stamp, std::string fieldName)
    : NodeFunction(std::move(stamp), "FieldAccess"), fieldName(std::move(fieldName)) {};

NodeFunctionFieldAccess::NodeFunctionFieldAccess(NodeFunctionFieldAccess* other) : NodeFunction(other), fieldName(other->getFieldName()) {};

std::shared_ptr<NodeFunction> NodeFunctionFieldAccess::create(DataType stamp, std::string fieldName)
{
    return std::make_shared<NodeFunctionFieldAccess>(NodeFunctionFieldAccess(std::move(stamp), std::move(fieldName)));
}

std::shared_ptr<NodeFunction> NodeFunctionFieldAccess::create(std::string fieldName)
{
    return create(DataTypeProvider::provideDataType(PhysicalType::Type::UNDEFINED), std::move(fieldName));
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
