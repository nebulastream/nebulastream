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
const auto float32 = DataTypeProvider::provideDataType(DataType::Type::FLOAT32);

const std::unordered_map<std::string_view, ImageManipFunction> functions = {
    {"ImageManipMono8ToJPG", {varSized, {varSized, uint64, uint64}}},
    {"ImageManipRectangle", {uint64, {uint64, uint64, uint64, uint64}}},
    {"ImageManipMono16ToMono8", {varSized, {varSized, uint64, uint64, uint16, uint16}}},
    {"ImageManipMono8ToYUYV", {varSized, {varSized, uint64, uint64}}},
    {"ImageManipMono16ToPNG16", {varSized, {varSized, uint64, uint64}}},
    {"ImageManipYUYVToJPG", {varSized, {varSized, uint64, uint64}}},
    {"ImageManipFaceDetection", {uint64, {varSized, uint64, uint64}}},
    {"ImageManipMono16MIN", {uint16, {varSized}}},
    {"ImageManipMono16MAX", {uint16, {varSized}}},
    {"ImageManipMono16AVG", {uint16, {varSized}}},
    {"ImageManipMono16ToCelsius", {float32, {uint16}}},
    {"ImageManipMono16ROI", {varSized, {varSized, uint64, uint64, uint64}}},
    {"ImageManipDeserialize", {varSized, {varSized, uint64, uint64, uint64}}},
    {"ImageManipSerialize", {varSized, {varSized, uint64, uint64, uint64}}},
    {"ImageManipDrawRectangle", {varSized, {varSized, uint64}}},
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

namespace LogicalFunctionGeneratedRegistrar
{
#define ImageManipFunction(name) \
    LogicalFunctionRegistryReturnType RegisterImageManip##name##LogicalFunction(LogicalFunctionRegistryArguments arguments) \
    { \
        return ImageManipLogicalFunction::create("ImageManip" #name, std::move(arguments.children)); \
    }

ImageManipFunction(Mono8ToJPG);
ImageManipFunction(Mono16ToMono8);
ImageManipFunction(YUYVToJPG);
ImageManipFunction(Mono16ToPNG16);
ImageManipFunction(Mono8ToYUYV);
ImageManipFunction(FaceDetection);
ImageManipFunction(Serialize);
ImageManipFunction(Deserialize);
ImageManipFunction(DrawRectangle);
ImageManipFunction(Mono16ROI);
ImageManipFunction(Mono16AVG);
ImageManipFunction(Mono16ToCelsius);
ImageManipFunction(Mono16MAX);
ImageManipFunction(Mono16MIN);
ImageManipFunction(Rectangle);
}
}
