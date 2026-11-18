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

#include <Functions/NullValueLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

NullValueLogicalFunction::NullValueLogicalFunction(const DataType& dataType)
    : dataType(DataTypeProvider::provideDataType(dataType.type, DataType::NULLABLE::IS_NULLABLE))
{
}

DataType NullValueLogicalFunction::getDataType() const
{
    return dataType;
}

NullValueLogicalFunction NullValueLogicalFunction::withDataType(const DataType& dataType) const
{
    return NullValueLogicalFunction{dataType};
}

LogicalFunction NullValueLogicalFunction::withInferredDataType(const Schema<Field, Unordered>&) const
{
    return *this;
}

std::vector<LogicalFunction> NullValueLogicalFunction::getChildren() const
{
    return {};
}

NullValueLogicalFunction NullValueLogicalFunction::withChildren(const std::vector<LogicalFunction>&) const
{
    return *this;
}

std::string_view NullValueLogicalFunction::getType() const
{
    return NAME;
}

bool NullValueLogicalFunction::operator==(const NullValueLogicalFunction& rhs) const
{
    return dataType == rhs.dataType;
}

std::string NullValueLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("NullValueLogicalFunction({})", dataType);
    }
    return "NULL";
}

Reflected Reflector<NullValueLogicalFunction>::operator()(const NullValueLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedNullValueLogicalFunction{.dataType = function.getDataType()});
}

NullValueLogicalFunction Unreflector<NullValueLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [dataType] = context.unreflect<detail::ReflectedNullValueLogicalFunction>(reflected);
    return NullValueLogicalFunction{dataType};
}

/// NOLINTBEGIN(performance-unnecessary-value-param)
LogicalFunctionRegistryReturnType NullValueLogicalFunction::createNullValue(LogicalFunctionRegistryArguments)
{
    PRECONDITION(false, "NullValue carries its type, so it is only built directly or via reflection, not using the registry");
    std::unreachable();
}

/// NOLINTEND(performance-unnecessary-value-param)

}
