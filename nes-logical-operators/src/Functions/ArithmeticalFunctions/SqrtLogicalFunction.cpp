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

#include <Functions/ArithmeticalFunctions/SqrtLogicalFunction.hpp>

#include <algorithm>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

SqrtLogicalFunction::SqrtLogicalFunction(const LogicalFunction& child) : dataType(child.getDataType()), child(child) { };

bool SqrtLogicalFunction::operator==(const SqrtLogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string SqrtLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("SqrtLogicalFunction({} : {})", child.explain(verbosity), dataType);
    }
    return fmt::format("SQRT({})", child.explain(verbosity));
}

DataType SqrtLogicalFunction::getDataType() const
{
    return dataType;
};

SqrtLogicalFunction SqrtLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction SqrtLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChildren = getChildren() | std::views::transform([&schema](auto& child) { return child.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();
    INVARIANT(newChildren.size() == 1, "SqrtLogicalFunction expects exactly two child function but has {}", newChildren.size());
    auto newDataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
    newDataType.nullable = std::ranges::any_of(newChildren, [](const auto& child) { return child.getDataType().nullable; });
    return withDataType(newDataType).withChildren(newChildren);
};

std::vector<LogicalFunction> SqrtLogicalFunction::getChildren() const
{
    return {child};
};

SqrtLogicalFunction SqrtLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "SqrtLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view SqrtLogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<SqrtLogicalFunction>::operator()(const SqrtLogicalFunction& function) const
{
    return reflect(detail::ReflectedSqrtLogicalFunction{.child = function.child});
}

SqrtLogicalFunction Unreflector<SqrtLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [child] = unreflect<detail::ReflectedSqrtLogicalFunction>(reflected);
    if (!child.has_value())
    {
        throw CannotDeserialize("Missing child function");
    }
    return SqrtLogicalFunction(child.value());
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterSqrtLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<SqrtLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("Function requires exactly one child, but got {}", arguments.children.size());
    }
    return SqrtLogicalFunction(arguments.children[0]);
}

}
