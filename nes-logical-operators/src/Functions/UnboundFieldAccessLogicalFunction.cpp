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

#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>

#include <folly/Hash.h>

#include <Schema/Schema.hpp>
#include <Util/Overloaded.hpp>
#include <LogicalFunctionRegistry.hpp>
#include "DataTypes/DataType.hpp"
#include "Identifiers/Identifier.hpp"

namespace NES
{

UnboundFieldAccessLogicalFunction::UnboundFieldAccessLogicalFunction(Identifier fieldName)
    : fieldName(std::move(fieldName)), dataType(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED)) { };

UnboundFieldAccessLogicalFunction::UnboundFieldAccessLogicalFunction(Identifier fieldName, DataType dataType)
    : fieldName(std::move(fieldName)), dataType(std::move(std::move(dataType))) { };

bool UnboundFieldAccessLogicalFunction::operator==(const UnboundFieldAccessLogicalFunction& rhs) const
{
    return this->fieldName == rhs.fieldName && this->dataType == rhs.dataType;
}

Identifier UnboundFieldAccessLogicalFunction::getFieldName() const
{
    return fieldName;
}

LogicalFunction UnboundFieldAccessLogicalFunction::withFieldName(Identifier field) const
{
    auto copy = *this;
    copy.fieldName = std::move(field);
    return copy;
}

std::string UnboundFieldAccessLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("UnboundFieldAccessLogicalFunction({})", fieldName);
    }
    return fmt::format("{}", fieldName);
}

LogicalFunction UnboundFieldAccessLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    const auto existingField = schema.getFieldByName(fieldName);
    if (!existingField)
    {
        throw CannotInferSchema("field {} is not part of the schema {}", fieldName, schema);
    }
    return FieldAccessLogicalFunction(existingField.value());
}

DataType UnboundFieldAccessLogicalFunction::getDataType() const
{
    return dataType;
};

std::vector<LogicalFunction> UnboundFieldAccessLogicalFunction::getChildren() const
{
    return {};
};

UnboundFieldAccessLogicalFunction UnboundFieldAccessLogicalFunction::withChildren(const std::vector<LogicalFunction>&) const
{
    return *this;
};

std::string_view UnboundFieldAccessLogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<UnboundFieldAccessLogicalFunction>::operator()(const UnboundFieldAccessLogicalFunction&) const
{
    PRECONDITION(false, "UnboundFieldAccessLogicalFunction cannot be reflected, replace it with a FieldAccessLogicalFunction");
    std::unreachable();
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterUnboundFieldAccessLogicalFunction(LogicalFunctionRegistryArguments)
{
    PRECONDITION(false, "UnboundFieldAccessLogicalFunction cannot be created via registry, instantiate it directly");
    std::unreachable();
}

UnboundFieldAccessLogicalFunction
Unreflector<UnboundFieldAccessLogicalFunction>::operator()(const Reflected&, const ReflectionContext&) const
{
    throw CannotDeserialize("UnboundFieldAccessLogicalFunction cannot be deserialized");
}
}

std::size_t
std::hash<NES::UnboundFieldAccessLogicalFunction>::operator()(const NES::UnboundFieldAccessLogicalFunction& function) const noexcept
{
    return folly::hash::hash_combine(function.getFieldName(), function.dataType);
}
