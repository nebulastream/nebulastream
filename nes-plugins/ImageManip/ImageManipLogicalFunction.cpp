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

#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <LogicalFunctionUnreflectionRegistry.hpp>
#include "DataTypes/DataType.hpp"

namespace NES
{
namespace
{
struct ImageManipFunction
{
    DataType returnValue;
    std::vector<DataType> argumentTypes;

    void check(std::string_view name, const std::vector<LogicalFunction>& arguments) const
    {
        if (arguments.size() != argumentTypes.size())
        {
            throw CannotInferSchema("{} expects {} parameter(s), but received {}.", name, argumentTypes.size(), arguments.size());
        }

        for (size_t index = 0; index < arguments.size(); ++index)
        {
            if (argumentTypes[index] != arguments[index].getDataType())
            {
                throw CannotInferSchema(
                    "{} expects parameter {} to be {}, but received {}.",
                    name,
                    index,
                    argumentTypes[index],
                    arguments[index].getDataType());
            }
        }
    }
};

const auto varSized = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
const auto uint16 = DataTypeProvider::provideDataType(DataType::Type::UINT16);
const auto uint64 = DataTypeProvider::provideDataType(DataType::Type::UINT64);
const auto nullableUint64 = DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE);
const auto float32 = DataTypeProvider::provideDataType(DataType::Type::FLOAT32);

const std::unordered_map<std::string_view, ImageManipFunction> functions = {
    {"IMAGE_MANIP_MONO8_TO_JPG", {varSized, {varSized, uint64, uint64}}},
    {"IMAGE_MANIP_RECTANGLE", {uint64, {uint64, uint64, uint64, uint64}}},
    {"IMAGE_MANIP_MONO16_TO_MONO8", {varSized, {varSized, uint64, uint64, uint16, uint16}}},
    {"IMAGE_MANIP_MONO8_TO_YUYV", {varSized, {varSized, uint64, uint64}}},
    {"IMAGE_MANIP_MONO16_TO_PNG16", {varSized, {varSized, uint64, uint64}}},
    {"IMAGE_MANIP_MONO16_TO_JPG", {varSized, {varSized, uint64, uint64}}},
    {"IMAGE_MANIP_MONO16_TO_YUYV", {varSized, {varSized, uint64, uint64}}},
    {"IMAGE_MANIP_YUYV_TO_JPG", {varSized, {varSized, uint64, uint64}}},
    {"IMAGE_MANIP_FACE_DETECTION", {nullableUint64, {varSized, uint64, uint64}}},
    {"IMAGE_MANIP_MONO16_MIN", {uint16, {varSized}}},
    {"IMAGE_MANIP_MONO16_MAX", {uint16, {varSized}}},
    {"IMAGE_MANIP_MONO16_AVG", {uint16, {varSized}}},
    {"IMAGE_MANIP_MONO16_TO_CELSIUS", {float32, {uint16}}},
    {"IMAGE_MANIP_MONO16_ROI", {varSized, {varSized, uint64, uint64, uint64}}},
    {"IMAGE_MANIP_DESERIALIZE", {varSized, {varSized, uint64, uint64, uint64}}},
    {"IMAGE_MANIP_SERIALIZE", {varSized, {varSized, uint64, uint64, uint64}}},
    {"IMAGE_MANIP_DRAW_RECTANGLE", {varSized, {varSized, uint64}}},
};
}

class ImageManipLogicalFunction final
{
public:
    ImageManipLogicalFunction(DataType dataType, std::string functionName, std::vector<LogicalFunction> children)
        : functionName(std::move(functionName)), children(std::move(children)), dataType(std::move(dataType))
    {
    }

    static LogicalFunction create(std::string_view functionName, std::vector<LogicalFunction> children)
    {
        const auto function = functions.find(functionName);
        if (function == functions.end())
        {
            throw FunctionNotImplemented("'{}' does not exist", functionName);
        }
        return ImageManipLogicalFunction(function->second.returnValue, std::string(functionName), std::move(children));
    }

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

    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const
    {
        auto copy = *this;
        for (auto& child : copy.children)
        {
            child = child.withInferredDataType(schema);
        }
        functions.at(copy.functionName).check(copy.functionName, copy.children);
        copy.dataType.nullable = std::ranges::any_of(copy.children, [](const auto& child) { return child.getDataType().nullable; });
        return copy;
    }

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const { return children; }

    [[nodiscard]] ImageManipLogicalFunction withChildren(const std::vector<LogicalFunction>& newChildren) const
    {
        auto copy = *this;
        copy.children = newChildren;
        return copy;
    }

    [[nodiscard]] std::string_view getType() const { return functionName; }

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
    std::vector<LogicalFunction> children;
};
}

template <>
struct Reflector<ImageManipLogicalFunction>
{
    Reflected operator()(const ImageManipLogicalFunction& function, const ReflectionContext& context) const
    {
        return context.reflect(detail::ReflectedImageManipLogicalFunction{.children = function.children});
    }
};

static_assert(LogicalFunctionConcept<ImageManipLogicalFunction>);

const auto imageManipUnreflectionRegistration = []
{
    for (const auto& [name, unused] : functions)
    {
        static_cast<void>(unused);
        const bool registered = LogicalFunctionUnreflectionRegistry::instance().addUnreflectorEntry(
            std::string{name},
            [name](const Reflected& reflected, const ReflectionContext& context)
            {
                auto [children] = context.unreflect<detail::ReflectedImageManipLogicalFunction>(reflected);
                return ImageManipLogicalFunction::create(name, std::move(children));
            });
        INVARIANT(registered, "Duplicate image manipulation unreflector for {}", name);
    }
    return true;
}();

void registerImageManipLogicalFunctionUnreflectors()
{
    static_cast<void>(imageManipUnreflectionRegistration);
}

namespace LogicalFunctionGeneratedRegistrar
{
#define ImageManipFunction(name) \
    LogicalFunctionRegistryReturnType Register##name##LogicalFunction(LogicalFunctionRegistryArguments arguments) \
    { \
        return ImageManipLogicalFunction::create(#name, std::move(arguments.children)); \
    }

ImageManipFunction(IMAGE_MANIP_MONO8_TO_JPG);
ImageManipFunction(IMAGE_MANIP_MONO16_TO_MONO8);
ImageManipFunction(IMAGE_MANIP_YUYV_TO_JPG);
ImageManipFunction(IMAGE_MANIP_MONO16_TO_PNG16);
ImageManipFunction(IMAGE_MANIP_MONO16_TO_JPG);
ImageManipFunction(IMAGE_MANIP_MONO16_TO_YUYV);
ImageManipFunction(IMAGE_MANIP_MONO8_TO_YUYV);
ImageManipFunction(IMAGE_MANIP_FACE_DETECTION);
ImageManipFunction(IMAGE_MANIP_SERIALIZE);
ImageManipFunction(IMAGE_MANIP_DESERIALIZE);
ImageManipFunction(IMAGE_MANIP_DRAW_RECTANGLE);
ImageManipFunction(IMAGE_MANIP_MONO16_ROI);
ImageManipFunction(IMAGE_MANIP_MONO16_AVG);
ImageManipFunction(IMAGE_MANIP_MONO16_TO_CELSIUS);
ImageManipFunction(IMAGE_MANIP_MONO16_MAX);
ImageManipFunction(IMAGE_MANIP_MONO16_MIN);
ImageManipFunction(IMAGE_MANIP_RECTANGLE);
}
}
