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

#include "FormatLogicalFunction.hpp"

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
#include <Serialization/DataTypeSerializationUtil.hpp> /// NOLINT(misc-include-cleaner)
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h> /// NOLINT(misc-include-cleaner)

namespace NES
{

FormatLogicalFunction::FormatLogicalFunction(std::vector<LogicalFunction> children)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED)), children(std::move(children))
{
}

bool FormatLogicalFunction::operator==(const FormatLogicalFunction& rhs) const
{
    return children == rhs.children;
}

std::string FormatLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    const auto rendered
        = children | std::views::transform([verbosity](const auto& child) { return child.explain(verbosity); }) | std::ranges::to<std::vector>();
    return fmt::format("Format({})", fmt::join(rendered, ", "));
}

DataType FormatLogicalFunction::getDataType() const
{
    return dataType;
};

FormatLogicalFunction FormatLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction FormatLogicalFunction::withInferredDataType(const Schema& schema) const
{
    auto newChildren = getChildren() | std::views::transform([&schema](auto& child) { return child.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();
    INVARIANT(not newChildren.empty(), "FormatLogicalFunction expects at least one child (the format string)");

    /// TODO(student): consider validating that newChildren[0] is a VARSIZED format string.
    ///                You may also wrap non-VARSIZED placeholder args here in a to_string(...)
    ///                helper so the physical side only deals with VARSIZED inputs.
    ///                For inspiration see the FmtLogicalFunction prototype on branch
    ///                `origin/retrospective-study-charite-mlife-rebased-v2` (file
    ///                `nes-plugins/Functions/Fmt/FmtLogicalFunction.cpp`, commit e88b3b46ec).

    auto newDataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
    newDataType.nullable = std::ranges::any_of(newChildren, [](const auto& child) { return child.getDataType().nullable; });
    return withDataType(newDataType).withChildren(newChildren);
};

std::vector<LogicalFunction> FormatLogicalFunction::getChildren() const
{
    return children;
};

FormatLogicalFunction FormatLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(not children.empty(), "FormatLogicalFunction requires at least one child (the format string)");
    auto copy = *this;
    copy.children = children;
    return copy;
};

std::string_view FormatLogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<FormatLogicalFunction>::operator()(const FormatLogicalFunction& function) const
{
    return reflect(detail::ReflectedFormatLogicalFunction{.children = function.children});
}

FormatLogicalFunction Unreflector<FormatLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [children] = unreflect<detail::ReflectedFormatLogicalFunction>(reflected);
    if (children.empty())
    {
        throw CannotDeserialize("FormatLogicalFunction is missing its children");
    }
    return FormatLogicalFunction{std::move(children)};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterFormatLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<FormatLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.empty())
    {
        throw CannotDeserialize("Format requires at least one child (the format string)");
    }
    return FormatLogicalFunction(std::move(arguments.children));
}

}
