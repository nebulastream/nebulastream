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


#include <Functions/UnnestLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp> /// NOLINT(misc-include-cleaner)
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h> /// NOLINT(misc-include-cleaner)

namespace NES
{

/// NOLINTNEXTLINE(modernize-pass-by-value)
UnnestLogicalFunction::UnnestLogicalFunction(const LogicalFunction& child)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED)), child(child)
{
}

bool UnnestLogicalFunction::operator==(const UnnestLogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string UnnestLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("UNNEST({})", child.explain(verbosity));
}

DataType UnnestLogicalFunction::getDataType() const
{
    return dataType;
}

UnnestLogicalFunction UnnestLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

LogicalFunction UnnestLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    const auto inferred = child.withInferredDataType(schema);
    if (not inferred.getDataType().isType(DataType::Type::VARSIZED))
    {
        throw DifferentFieldTypeExpected("UNNEST expects a VARSIZED JSON array, but got {}", inferred.getDataType());
    }
    if (inferred.getDataType().nullable)
    {
        throw UnsupportedQuery("UNNEST does not support nullable inputs, but got {}", inferred);
    }
    /// An element is never null: a JSON null element is the text `null`.
    return withDataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED)).withChildren({inferred});
}

std::vector<LogicalFunction> UnnestLogicalFunction::getChildren() const
{
    return {child};
}

LogicalFunction UnnestLogicalFunction::getChild() const
{
    return child;
}

UnnestLogicalFunction UnnestLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "UNNEST has one child, got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::string_view UnnestLogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<UnnestLogicalFunction>::operator()(const UnnestLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedUnnestLogicalFunction{.child = function.child});
}

UnnestLogicalFunction Unreflector<UnnestLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [child] = context.unreflect<detail::ReflectedUnnestLogicalFunction>(reflected);
    if (!child.has_value())
    {
        throw CannotDeserialize("UnnestLogicalFunction is missing its child");
    }
    return UnnestLogicalFunction{child.value()};
}

LogicalFunctionRegistryReturnType UnnestLogicalFunction::createUNNEST(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 1)
    {
        throw InvalidQuerySyntax("UNNEST expects one argument, but got {}", arguments.children.size());
    }
    return UnnestLogicalFunction(arguments.children.front());
}

}
