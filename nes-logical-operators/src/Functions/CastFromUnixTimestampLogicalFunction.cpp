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

#include <Functions/CastFromUnixTimestampLogicalFunction.hpp>

#include <algorithm>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

CastFromUnixTimestampLogicalFunction::CastFromUnixTimestampLogicalFunction(LogicalFunction child)
    : outputType(LogicalType{"UNDEFINED", {}, Nullable::IS_NULLABLE}), child(std::move(child))
{
}

bool CastFromUnixTimestampLogicalFunction::operator==(const CastFromUnixTimestampLogicalFunction& rhs) const
{
    return this->outputType == rhs.outputType && this->child == rhs.child;
}

LogicalType CastFromUnixTimestampLogicalFunction::getLogicalType() const
{
    return outputType;
}

CastFromUnixTimestampLogicalFunction CastFromUnixTimestampLogicalFunction::withLogicalType(const LogicalType& logicalType) const
{
    auto copy = *this;
    copy.outputType = logicalType;
    return copy;
}

LogicalFunction CastFromUnixTimestampLogicalFunction::withInferredLogicalType(const Schema& schema) const
{
    const auto newChildren = getChildren() | std::views::transform([&schema](auto& child) { return child.withInferredLogicalType(schema); })
        | std::ranges::to<std::vector>();
    INVARIANT(newChildren.size() == 1, "CastFromUnixTimestampLogicalFunction expects exactly one child function but has {}", newChildren.size());
    auto childType = newChildren[0].getLogicalType();
    /// TODO(datatype-migration): the prototype LogicalType vocabulary lacks a dedicated unsigned
    /// 64-bit timestamp; we accept INTEGER as the stand-in.
    if (not childType.isUndefined() and not childType.isInteger())
    {
        throw DifferentFieldTypeExpected("CASTFROMUNIXTS expects an INTEGER input, but got {}", childType);
    }
    return withLogicalType(LogicalType{"TEXT", {}, childType.getNullable()}).withChildren(newChildren);
}

std::vector<LogicalFunction> CastFromUnixTimestampLogicalFunction::getChildren() const
{
    return {child};
}

CastFromUnixTimestampLogicalFunction CastFromUnixTimestampLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "CastFromUnixTimestampLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
}

std::string_view CastFromUnixTimestampLogicalFunction::getType()
{
    return NAME;
}

std::string CastFromUnixTimestampLogicalFunction::explain(ExplainVerbosity) const
{
    return fmt::format("Cast from unix timestamp (ms) to ISO-8601 UTC, outputType={}", outputType);
}

Reflected Reflector<CastFromUnixTimestampLogicalFunction>::operator()(const CastFromUnixTimestampLogicalFunction& function) const
{
    return reflect(detail::ReflectedCastFromUnixTimestampLogicalFunction{.child = function.child});
}

CastFromUnixTimestampLogicalFunction Unreflector<CastFromUnixTimestampLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [function] = unreflect<detail::ReflectedCastFromUnixTimestampLogicalFunction>(reflected);

    if (!function.has_value())
    {
        throw CannotDeserialize("Failed to deserialize child of CastFromUnixTimestampLogicalFunction");
    }
    return CastFromUnixTimestampLogicalFunction(std::move(function.value()));
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterCastFromUnixTsLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<CastFromUnixTimestampLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("CastFromUnixTimestampLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    }
    return CastFromUnixTimestampLogicalFunction(arguments.children[0]);
}

}
