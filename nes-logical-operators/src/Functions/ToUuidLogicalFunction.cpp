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

#include <Functions/ToUuidLogicalFunction.hpp>

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

ToUuidLogicalFunction::ToUuidLogicalFunction(const LogicalFunction& hi, const LogicalFunction& lo)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED)), hi(hi), lo(lo)
{
}

bool ToUuidLogicalFunction::operator==(const ToUuidLogicalFunction& rhs) const
{
    return hi == rhs.hi and lo == rhs.lo;
}

std::string ToUuidLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("TO_UUID({}, {})", hi.explain(verbosity), lo.explain(verbosity));
}

DataType ToUuidLogicalFunction::getDataType() const
{
    return dataType;
}

ToUuidLogicalFunction ToUuidLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

LogicalFunction ToUuidLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChildren = getChildren() | std::views::transform([&schema](auto& child) { return child.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();
    INVARIANT(newChildren.size() == 2, "ToUuidLogicalFunction expects exactly two child functions but has {}", newChildren.size());

    for (size_t i = 0; i < 2; ++i)
    {
        const auto childType = newChildren[i].getDataType().type;
        if (childType != DataType::Type::UNDEFINED && childType != DataType::Type::UINT64)
        {
            throw DifferentFieldTypeExpected(
                "TO_UUID expects UINT64 arguments, but argument {} has type {}", i, newChildren[i].getDataType());
        }
    }

    auto newDataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
    newDataType.nullable = std::ranges::any_of(newChildren, [](const auto& child) { return child.getDataType().nullable; });
    return withDataType(newDataType).withChildren(newChildren);
}

std::vector<LogicalFunction> ToUuidLogicalFunction::getChildren() const
{
    return {hi, lo};
}

ToUuidLogicalFunction ToUuidLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2, "ToUuidLogicalFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.hi = children[0];
    copy.lo = children[1];
    return copy;
}

std::string_view ToUuidLogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<ToUuidLogicalFunction>::operator()(const ToUuidLogicalFunction& function) const
{
    return reflect(detail::ReflectedToUuidLogicalFunction{.hi = function.hi, .lo = function.lo});
}

ToUuidLogicalFunction Unreflector<ToUuidLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [hi, lo] = unreflect<detail::ReflectedToUuidLogicalFunction>(reflected);
    if (!hi.has_value() || !lo.has_value())
    {
        throw CannotDeserialize("ToUuidLogicalFunction is missing a child");
    }
    return ToUuidLogicalFunction{hi.value(), lo.value()};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterToUuidLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<ToUuidLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() < 2)
    {
        throw CannotDeserialize("ToUuidLogicalFunction requires two children, but only got {}", arguments.children.size());
    }
    return ToUuidLogicalFunction(*(arguments.children.end() - 2), *(arguments.children.end() - 1));
}

}
