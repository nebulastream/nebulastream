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

#pragma once

#include <algorithm>
#include <cstddef>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{
class ImageManipLogicalFunction final
{
public:
    ImageManipLogicalFunction(DataType dataType, std::string functionName, std::vector<LogicalFunction> children)
        : functionName(std::move(functionName)), children(std::move(children)), dataType(std::move(dataType))
    {
    }

    static LogicalFunction create(std::string_view functionName, std::vector<LogicalFunction> children);

    [[nodiscard]] bool operator==(const ImageManipLogicalFunction& rhs) const
    {
        return functionName == rhs.functionName && children == rhs.children && dataType == rhs.dataType;
    }

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const
    {
        return fmt::format(
            "{}({})",
            functionName,
            fmt::join(children | std::views::transform([&](const auto& child) { return child.explain(verbosity); }), ", "));
    }

    [[nodiscard]] DataType getDataType() const { return dataType; }

    [[nodiscard]] ImageManipLogicalFunction withDataType(const DataType& newDataType) const
    {
        auto copy = *this;
        copy.dataType = newDataType;
        return copy;
    }

    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const { return children; }

    [[nodiscard]] ImageManipLogicalFunction withChildren(const std::vector<LogicalFunction>& newChildren) const
    {
        auto copy = *this;
        copy.children = newChildren;
        return copy;
    }

    [[nodiscard]] std::string_view getType() const { return functionName; }

    static LogicalFunctionRegistryReturnType createIMAGE_MANIP_MONO8_TO_JPG(LogicalFunctionRegistryArguments arguments);
    static LogicalFunctionRegistryReturnType createIMAGE_MANIP_MONO16_TO_MONO8(LogicalFunctionRegistryArguments arguments);
    static LogicalFunctionRegistryReturnType createIMAGE_MANIP_YUYV_TO_JPG(LogicalFunctionRegistryArguments arguments);
    static LogicalFunctionRegistryReturnType createIMAGE_MANIP_MONO16_TO_PNG16(LogicalFunctionRegistryArguments arguments);
    static LogicalFunctionRegistryReturnType createIMAGE_MANIP_MONO16_TO_JPG(LogicalFunctionRegistryArguments arguments);
    static LogicalFunctionRegistryReturnType createIMAGE_MANIP_MONO16_TO_YUYV(LogicalFunctionRegistryArguments arguments);
    static LogicalFunctionRegistryReturnType createIMAGE_MANIP_MONO8_TO_YUYV(LogicalFunctionRegistryArguments arguments);
    static LogicalFunctionRegistryReturnType createIMAGE_MANIP_FACE_DETECTION(LogicalFunctionRegistryArguments arguments);
    static LogicalFunctionRegistryReturnType createIMAGE_MANIP_SERIALIZE(LogicalFunctionRegistryArguments arguments);
    static LogicalFunctionRegistryReturnType createIMAGE_MANIP_DESERIALIZE(LogicalFunctionRegistryArguments arguments);
    static LogicalFunctionRegistryReturnType createIMAGE_MANIP_DRAW_RECTANGLE(LogicalFunctionRegistryArguments arguments);
    static LogicalFunctionRegistryReturnType createIMAGE_MANIP_MONO16_ROI(LogicalFunctionRegistryArguments arguments);
    static LogicalFunctionRegistryReturnType createIMAGE_MANIP_MONO16_AVG(LogicalFunctionRegistryArguments arguments);
    static LogicalFunctionRegistryReturnType createIMAGE_MANIP_MONO16_TO_CELSIUS(LogicalFunctionRegistryArguments arguments);
    static LogicalFunctionRegistryReturnType createIMAGE_MANIP_MONO16_MAX(LogicalFunctionRegistryArguments arguments);
    static LogicalFunctionRegistryReturnType createIMAGE_MANIP_MONO16_MIN(LogicalFunctionRegistryArguments arguments);
    static LogicalFunctionRegistryReturnType createIMAGE_MANIP_RECTANGLE(LogicalFunctionRegistryArguments arguments);
    static LogicalFunctionRegistryReturnType createIMAGE_MANIP_AUDIO_TO_MFCC(LogicalFunctionRegistryArguments arguments);
    static LogicalFunctionRegistryReturnType createIMAGE_MANIP_ARGMAX_F32(LogicalFunctionRegistryArguments arguments);
    static LogicalFunctionRegistryReturnType createIMAGE_MANIP_MAX_F32(LogicalFunctionRegistryArguments arguments);
    static LogicalFunctionRegistryReturnType createIMAGE_MANIP_MAX_ABS_F32(LogicalFunctionRegistryArguments arguments);

private:
    std::string functionName;
    std::vector<LogicalFunction> children;
    DataType dataType;

    friend Reflector<ImageManipLogicalFunction>;
};

namespace detail
{
struct ReflectedImageManipLogicalFunction
{
    std::string functionName;
    std::vector<LogicalFunction> children;
};
}

template <>
struct Reflector<ImageManipLogicalFunction>
{
    Reflected operator()(const ImageManipLogicalFunction& function, const ReflectionContext& context) const
    {
        return context.reflect(
            detail::ReflectedImageManipLogicalFunction{.functionName = function.functionName, .children = function.children});
    }
};

static_assert(LogicalFunctionConcept<ImageManipLogicalFunction>);

template <>
struct Unreflector<ImageManipLogicalFunction>
{
    ImageManipLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};
}
