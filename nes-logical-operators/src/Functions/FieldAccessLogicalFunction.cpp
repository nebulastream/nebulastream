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

#include <Functions/FieldAccessLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{
FieldAccessLogicalFunction::FieldAccessLogicalFunction(std::string fieldName)
    : fieldName(std::move(fieldName)), logicalType(LogicalType{"UNDEFINED", {}, Nullable::IS_NULLABLE}) { };

FieldAccessLogicalFunction::FieldAccessLogicalFunction(LogicalType logicalType, std::string fieldName)
    : fieldName(std::move(fieldName)), logicalType(std::move(logicalType)) { };

bool FieldAccessLogicalFunction::operator==(const FieldAccessLogicalFunction& rhs) const
{
    const bool fieldNamesMatch = rhs.fieldName == this->fieldName;
    const bool typesMatch = rhs.logicalType == this->logicalType;
    return fieldNamesMatch and typesMatch;
}

bool operator!=(const FieldAccessLogicalFunction& lhs, const FieldAccessLogicalFunction& rhs)
{
    return !(lhs == rhs);
}

std::string FieldAccessLogicalFunction::getFieldName() const
{
    return fieldName;
}

FieldAccessLogicalFunction FieldAccessLogicalFunction::withFieldName(const std::string_view newFieldName) const
{
    auto copy = *this;
    copy.fieldName = newFieldName;
    return copy;
}

std::string FieldAccessLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("FieldAccessLogicalFunction({}: {})", fieldName, logicalType);
    }
    return fieldName;
}

LogicalFunction FieldAccessLogicalFunction::withInferredLogicalType(const Schema& schema) const
{
    const auto existingField = schema.getFieldByName(fieldName);
    if (!existingField)
    {
        throw CannotInferSchema("field {} is not part of the schema {}", fieldName, schema);
    }

    auto copy = *this;
    copy.logicalType = existingField.value().logicalType;
    copy.fieldName = existingField.value().name;
    return copy;
}

LogicalType FieldAccessLogicalFunction::getLogicalType() const
{
    return logicalType;
};

FieldAccessLogicalFunction FieldAccessLogicalFunction::withLogicalType(const LogicalType& logicalType) const
{
    auto copy = *this;
    copy.logicalType = logicalType;
    return copy;
}

std::vector<LogicalFunction> FieldAccessLogicalFunction::getChildren() const
{
    return {};
};

FieldAccessLogicalFunction FieldAccessLogicalFunction::withChildren(const std::vector<LogicalFunction>&) const
{
    return *this;
};

std::string_view FieldAccessLogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<FieldAccessLogicalFunction>::operator()(const FieldAccessLogicalFunction& function) const
{
    return reflect(
        detail::ReflectedFieldAccessLogicalFunction{.fieldName = function.getFieldName(), .logicalType = function.getLogicalType()});
}

FieldAccessLogicalFunction Unreflector<FieldAccessLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [name, type] = unreflect<detail::ReflectedFieldAccessLogicalFunction>(reflected);
    return FieldAccessLogicalFunction{type, name};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterFieldAccessLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<FieldAccessLogicalFunction>(arguments.reflected);
    }

    PRECONDITION(false, "Function is only build directly via parser or via reflection, not using the registry");
    std::unreachable();
}

}
