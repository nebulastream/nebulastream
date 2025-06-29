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
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

class ImageManipLogicalFunction final : public LogicalFunctionConcept
{
    std::string functionName;
    std::vector<LogicalFunction> children;
    DataType dataType;
    ImageManipLogicalFunction(DataType stamp, std::string type, std::vector<LogicalFunction> children);

public:
    static LogicalFunction create(std::string_view functionName, std::vector<LogicalFunction> children);

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;
    [[nodiscard]] DataType getDataType() const override;
    [[nodiscard]] LogicalFunction withDataType(const DataType& dataType) const override;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const override;
    [[nodiscard]] std::vector<LogicalFunction> getChildren() const override;
    [[nodiscard]] LogicalFunction withChildren(const std::vector<LogicalFunction>& children) const override;
    [[nodiscard]] std::string_view getType() const override;
    [[nodiscard]] SerializableFunction serialize() const override;
    [[nodiscard]] bool operator==(const LogicalFunctionConcept& rhs) const override;
};


struct ImageManipFunction
{
    explicit ImageManipFunction(DataType returnValue, std::vector<DataType> argumentTypes)
        : returnValue(std::move(returnValue)), argumentTypes(std::move(argumentTypes))
    {
    }
    DataType returnValue;
    std::vector<DataType> argumentTypes;

    DataType initialStamp() const { return returnValue; }

    void check(std::string_view name, std::span<LogicalFunction> args) const
    {
        auto types = args | std::views::transform(&LogicalFunction::getDataType);

        if (args.size() != argumentTypes.size())
        {
            throw CannotInferSchema(
                "Error during stamp inference. {} expects {} parameter(s), but received {}.", name, argumentTypes.size(), types.size());
        }

        auto missMatches = std::views::zip(argumentTypes, types)
            | std::views::filter([](const auto& p) { return std::get<0>(p) != std::get<1>(p); }) | std::ranges::to<std::vector>();

        if (!missMatches.empty())
        {
            throw CannotInferSchema(
                "Error during stamp inference. {} expects parameter(s): {}\nBut received: {}",
                name,
                fmt::join(argumentTypes, ", "),
                fmt::join(types, ", "));
        }
    }
};

const static std::unordered_map<std::string_view, ImageManipFunction> Functions
    = {{"ImageManipFromBase64",
        ImageManipFunction(
            DataTypeProvider::provideDataType(DataType::Type::VARSIZED),
            std::vector{DataTypeProvider::provideDataType(DataType::Type::VARSIZED)})},

       {"ImageManipToBase64",
        ImageManipFunction(
            DataTypeProvider::provideDataType(DataType::Type::VARSIZED),
            std::vector{DataTypeProvider::provideDataType(DataType::Type::VARSIZED)})}};

ImageManipLogicalFunction::ImageManipLogicalFunction(DataType stamp, std::string type, std::vector<LogicalFunction> children)
    : functionName(std::move(type)), children(std::move(children)), dataType(std::move(stamp))
{
}
std::string ImageManipLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format(
        "{}({})",
        this->functionName,
        fmt::join(this->children | std::views::transform([&](const auto& child) { return child.explain(verbosity); }), ", "));
}
DataType ImageManipLogicalFunction::getDataType() const
{
    return dataType;
}
LogicalFunction ImageManipLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}
LogicalFunction ImageManipLogicalFunction::create(std::string_view functionName, std::vector<LogicalFunction> children)
{
    auto function = Functions.find(functionName);
    if (function == Functions.end())
    {
        throw FunctionNotImplemented(fmt::format("does not exist", functionName));
    }

    return LogicalFunction(ImageManipLogicalFunction(function->second.initialStamp(), std::string(functionName), std::move(children)));
}

LogicalFunction ImageManipLogicalFunction::withInferredDataType(const Schema& schema) const
{
    auto copy = *this;
    copy.children = std::views::transform(std::move(copy.children), [&](auto child) { return child.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();

    Functions.at(functionName).check(functionName, copy.children);
    return copy;
}
std::vector<LogicalFunction> ImageManipLogicalFunction::getChildren() const
{
    return children;
}
LogicalFunction ImageManipLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}
std::string_view ImageManipLogicalFunction::getType() const
{
    return functionName;
}
SerializableFunction ImageManipLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(getType());
    for (const auto& child : children)
    {
        serializedFunction.add_children()->CopyFrom(child.serialize());
    }
    DataTypeSerializationUtil::serializeDataType(getDataType(), serializedFunction.mutable_data_type());
    return serializedFunction;
}
bool ImageManipLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (auto castedRhs = dynamic_cast<const ImageManipLogicalFunction*>(&rhs))
    {
        return castedRhs->functionName == this->functionName && castedRhs->children == this->children
            && castedRhs->dataType == this->dataType;
    }
    return false;
}

namespace LogicalFunctionGeneratedRegistrar
{
#define ImageManipFunction(name) \
    LogicalFunctionRegistryReturnType RegisterImageManip##name##LogicalFunction(LogicalFunctionRegistryArguments arguments) \
    { \
        auto function = ImageManipLogicalFunction::create("ImageManip" #name, std::move(arguments.children)); \
        return function; \
    }

ImageManipFunction(ToBase64);
ImageManipFunction(FromBase64);
}
}
