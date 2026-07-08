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

#include <ToRGBLogicalFunction.hpp>

#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
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

namespace
{
/// Builds the RGBFrame STRUCT layout for a given pixel count. Mirrors
/// `makeRGBFrame` in `ImageDataType.cpp` — keep the two in sync.
DataType makeRGBFrameLayout(uint32_t pixelCount, DataType::NULLABLE nullable)
{
    const DataType channel{
        DataType::Type::FIXEDSIZED, DataType::NULLABLE::NOT_NULLABLE, DataType{DataType::Type::UINT8, DataType::NULLABLE::NOT_NULLABLE}, pixelCount};
    std::vector<std::pair<std::string, DataType>> fields;
    fields.emplace_back("r", channel);
    fields.emplace_back("g", channel);
    fields.emplace_back("b", channel);
    return DataType{DataType::Type::STRUCT, nullable, std::string{"RGBFrame"}, std::move(fields)};
}
}

ToRGBLogicalFunction::ToRGBLogicalFunction(const LogicalFunction& frame, const LogicalFunction& colormap)
    : dataType(DataType::Type::UNDEFINED, DataType::NULLABLE::NOT_NULLABLE), frame(frame), colormap(colormap)
{
}

DataType ToRGBLogicalFunction::getDataType() const
{
    return dataType;
}

ToRGBLogicalFunction ToRGBLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

LogicalFunction ToRGBLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChildren = getChildren() | std::views::transform([&schema](auto& c) { return c.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();
    INVARIANT(newChildren.size() == 2, "ToRGBLogicalFunction expects exactly two child functions but has {}", newChildren.size());

    /// Layout gate on child0: any STRUCT with a single FIXEDSIZED<UINT16>
    /// 'pixels' field. Output pixel count derives from it.
    const auto frameType = newChildren[0].getDataType();
    if (frameType.type != DataType::Type::STRUCT || frameType.fields.size() != 1
        || frameType.fields[0].first != "pixels"
        || frameType.fields[0].second.type != DataType::Type::FIXEDSIZED
        || frameType.fields[0].second.elementType[0].type != DataType::Type::UINT16)
    {
        throw DifferentFieldTypeExpected(
            "to_rgb expects a STRUCT with a single FIXEDSIZED<UINT16> 'pixels' field, got {}", frameType);
    }

    /// Colormap selector: a VARSIZED string. Concrete value is resolved at
    /// physical execution time, so we only check the type here.
    const auto colormapType = newChildren[1].getDataType();
    if (colormapType.type != DataType::Type::VARSIZED)
    {
        throw DifferentFieldTypeExpected("to_rgb expects a VARSIZED colormap name as second argument, got {}", colormapType);
    }

    const auto pixelCount = frameType.fields[0].second.count;
    const auto nullable = frameType.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE;
    auto newDataType = makeRGBFrameLayout(pixelCount, nullable);
    return withDataType(newDataType).withChildren(newChildren);
}

std::vector<LogicalFunction> ToRGBLogicalFunction::getChildren() const
{
    return {frame, colormap};
}

ToRGBLogicalFunction ToRGBLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2, "ToRGBLogicalFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.frame = children[0];
    copy.colormap = children[1];
    return copy;
}

std::string_view ToRGBLogicalFunction::getType() const
{
    return NAME;
}

bool ToRGBLogicalFunction::operator==(const ToRGBLogicalFunction& rhs) const
{
    return frame == rhs.frame && colormap == rhs.colormap;
}

std::string ToRGBLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("ToRGBLogicalFunction({}, {} : {})", frame.explain(verbosity), colormap.explain(verbosity), dataType);
    }
    return fmt::format("to_rgb({}, {})", frame.explain(verbosity), colormap.explain(verbosity));
}

Reflected Reflector<ToRGBLogicalFunction>::operator()(const ToRGBLogicalFunction& function) const
{
    return reflect(detail::ReflectedToRGBLogicalFunction{.frame = function.frame, .colormap = function.colormap});
}

ToRGBLogicalFunction Unreflector<ToRGBLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [frame, colormap] = unreflect<detail::ReflectedToRGBLogicalFunction>(reflected);
    if (!frame.has_value() || !colormap.has_value())
    {
        throw CannotDeserialize("ToRGBLogicalFunction is missing one of its children");
    }
    return ToRGBLogicalFunction(frame.value(), colormap.value());
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterTO_RGBLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<ToRGBLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("ToRGBLogicalFunction requires exactly two children, but got {}", arguments.children.size());
    }
    return ToRGBLogicalFunction(arguments.children[0], arguments.children[1]);
}

}
