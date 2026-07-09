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

#include <Functions/UDFCallLogicalFunction.hpp>

#include <algorithm>
#include <cstddef>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <UdfDescriptor.hpp>

namespace NES
{

UDFCallLogicalFunction::UDFCallLogicalFunction(std::string udfName, std::vector<LogicalFunction> arguments)
    : udfName(std::move(udfName)), arguments(std::move(arguments)), dataType(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED))
{
}

UDFCallLogicalFunction::UDFCallLogicalFunction(std::string udfName, UdfDescriptor descriptor, std::vector<LogicalFunction> arguments)
    : udfName(std::move(udfName))
    , descriptor(std::move(descriptor))
    , arguments(std::move(arguments))
    , dataType(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED))
{
}

bool UDFCallLogicalFunction::operator==(const UDFCallLogicalFunction& rhs) const
{
    return udfName == rhs.udfName && descriptor == rhs.descriptor && arguments == rhs.arguments;
}

std::string UDFCallLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    auto args = arguments | std::views::transform([&](const auto& arg) { return arg.explain(verbosity); });
    return fmt::format("{}({})", udfName, fmt::join(args, ", "));
}

DataType UDFCallLogicalFunction::getDataType() const
{
    return dataType;
}

LogicalFunction UDFCallLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    if (!descriptor.has_value())
    {
        /// The UDFResolutionRule must attach the descriptor before type inference runs.
        throw TypeInferenceException("UDF '{}' was not resolved against the UDF catalog before type inference", udfName);
    }
    auto copy = *this;
    for (auto& argument : copy.arguments)
    {
        argument = argument.withInferredDataType(schema);
    }
    if (copy.arguments.size() != descriptor->getArgTypes().size())
    {
        throw TypeInferenceException(
            "UDF '{}' expects {} argument(s) but was called with {}", udfName, descriptor->getArgTypes().size(), copy.arguments.size());
    }
    copy.dataType = descriptor->getReturnType();
    /// A scalar UDF may return NULL (e.g. Python `None`) regardless of its argument nullability, so the
    /// result is always nullable.
    copy.dataType.nullable = true;
    return copy;
}

std::vector<LogicalFunction> UDFCallLogicalFunction::getChildren() const
{
    return arguments;
}

UDFCallLogicalFunction UDFCallLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    auto copy = *this;
    copy.arguments = children;
    return copy;
}

std::string_view UDFCallLogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<UDFCallLogicalFunction>::operator()(const UDFCallLogicalFunction& function) const
{
    return reflect(detail::ReflectedUDFCallLogicalFunction{
        .udfName = function.udfName, .descriptor = function.descriptor, .arguments = function.arguments});
}

UDFCallLogicalFunction Unreflector<UDFCallLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [udfName, descriptor, arguments] = context.unreflect<detail::ReflectedUDFCallLogicalFunction>(reflected);
    if (descriptor.has_value())
    {
        return UDFCallLogicalFunction{std::move(udfName), std::move(descriptor).value(), std::move(arguments)};
    }
    return UDFCallLogicalFunction{std::move(udfName), std::move(arguments)};
}

/// UDFCall functions are never created through the function registry: the SQL parser constructs the
/// name-only placeholder directly, the UDFResolutionRule resolves it, and serialization round-trips
/// through the unreflection registry (which carries the full descriptor). This entry exists only to
/// satisfy the plugin convention.
LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterUDFCallLogicalFunction(LogicalFunctionRegistryArguments)
{
    throw FunctionNotImplemented("UDFCall functions are created by the SQL parser and resolved against the UDF catalog, not the registry");
}

}
