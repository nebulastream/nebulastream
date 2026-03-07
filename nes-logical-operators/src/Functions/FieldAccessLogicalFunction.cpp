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

namespace NES
{
FieldAccessLogicalFunction::FieldAccessLogicalFunction(std::string fieldName)
    : fieldName(std::move(fieldName)), dataType(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED)) { };

FieldAccessLogicalFunction::FieldAccessLogicalFunction(DataType dataType, std::string fieldName)
    : fieldName(std::move(fieldName)), dataType(std::move(dataType)) { };

bool FieldAccessLogicalFunction::operator==(const FieldAccessLogicalFunction& rhs) const
{
    const bool fieldNamesMatch = rhs.fieldName == this->fieldName;
    const bool dataTypesMatch = rhs.dataType == this->dataType;
    return fieldNamesMatch and dataTypesMatch;
}

bool operator!=(const FieldAccessLogicalFunction& lhs, const FieldAccessLogicalFunction& rhs)
{
    return !(lhs == rhs);
}

std::string FieldAccessLogicalFunction::getFieldName() const
{
    return fieldName;
}

FieldAccessLogicalFunction FieldAccessLogicalFunction::withFieldName(std::string fieldName) const
{
    auto copy = *this;
    copy.fieldName = std::move(fieldName);
    return copy;
}

std::string FieldAccessLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("FieldAccessLogicalFunction({}{})", fieldName, dataType);
    }
    return fieldName;
}

LogicalFunction FieldAccessLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto existingField = schema.getFieldByName(fieldName);
    if (!existingField)
    {
        throw CannotInferSchema("field {} is not part of the schema {}", fieldName, schema);
    }

    auto copy = *this;
    copy.dataType = existingField.value().dataType;
    copy.fieldName = existingField.value().name;
    return copy;
}

DataType FieldAccessLogicalFunction::getDataType() const
{
    return dataType;
};

FieldAccessLogicalFunction FieldAccessLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
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
        detail::ReflectedFieldAccessLogicalFunction{.fieldName = function.getFieldName(), .dataType = function.getDataType().type});
}

FieldAccessLogicalFunction
Unreflector<FieldAccessLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [name, type] = context.unreflect<detail::ReflectedFieldAccessLogicalFunction>(reflected);

    return FieldAccessLogicalFunction{DataType{type}, name};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterFieldAccessLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return ReflectionContext{}.unreflect<FieldAccessLogicalFunction>(arguments.reflected);
    }

    PRECONDITION(false, "Function is only build directly via parser or via reflection, not using the registry");
    std::unreachable();
}

}
