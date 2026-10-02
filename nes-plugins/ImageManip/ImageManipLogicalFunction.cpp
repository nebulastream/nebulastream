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

#include <ImageManipLogicalFunction.hpp>

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

const std::unordered_map<std::string_view, ImageManipFunction>& imageManipFunctions()
{
    static const auto varSized = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
    static const auto uint16 = DataTypeProvider::provideDataType(DataType::Type::UINT16);
    static const auto uint64 = DataTypeProvider::provideDataType(DataType::Type::UINT64);
    static const auto nullableUint64 = DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE);
    static const auto float32 = DataTypeProvider::provideDataType(DataType::Type::FLOAT32);

    static const std::unordered_map<std::string_view, ImageManipFunction> functions = {
        {"IMAGE_MANIP_MONO8_TO_JPG", {.returnValue = varSized, .argumentTypes = {varSized, uint64, uint64}}},
        {"IMAGE_MANIP_RECTANGLE", {.returnValue = uint64, .argumentTypes = {uint64, uint64, uint64, uint64}}},
        {"IMAGE_MANIP_MONO16_TO_MONO8", {.returnValue = varSized, .argumentTypes = {varSized, uint64, uint64, uint16, uint16}}},
        {"IMAGE_MANIP_MONO8_TO_YUYV", {.returnValue = varSized, .argumentTypes = {varSized, uint64, uint64}}},
        {"IMAGE_MANIP_MONO16_TO_PNG16", {.returnValue = varSized, .argumentTypes = {varSized, uint64, uint64}}},
        {"IMAGE_MANIP_MONO16_TO_JPG", {.returnValue = varSized, .argumentTypes = {varSized, uint64, uint64}}},
        {"IMAGE_MANIP_MONO16_TO_YUYV", {.returnValue = varSized, .argumentTypes = {varSized, uint64, uint64}}},
        {"IMAGE_MANIP_YUYV_TO_JPG", {.returnValue = varSized, .argumentTypes = {varSized, uint64, uint64}}},
        {"IMAGE_MANIP_FACE_DETECTION", {.returnValue = nullableUint64, .argumentTypes = {varSized, uint64, uint64}}},
        {"IMAGE_MANIP_MONO16_MIN", {.returnValue = uint16, .argumentTypes = {varSized}}},
        {"IMAGE_MANIP_MONO16_MAX", {.returnValue = uint16, .argumentTypes = {varSized}}},
        {"IMAGE_MANIP_MONO16_AVG", {.returnValue = uint16, .argumentTypes = {varSized}}},
        {"IMAGE_MANIP_MONO16_TO_CELSIUS", {.returnValue = float32, .argumentTypes = {uint16}}},
        {"IMAGE_MANIP_MONO16_ROI", {.returnValue = varSized, .argumentTypes = {varSized, uint64, uint64, uint64}}},
        {"IMAGE_MANIP_DESERIALIZE", {.returnValue = varSized, .argumentTypes = {varSized, uint64, uint64, uint64}}},
        {"IMAGE_MANIP_SERIALIZE", {.returnValue = varSized, .argumentTypes = {varSized, uint64, uint64, uint64}}},
        {"IMAGE_MANIP_DRAW_RECTANGLE", {.returnValue = varSized, .argumentTypes = {varSized, uint64}}},
        {"IMAGE_MANIP_AUDIO_TO_MFCC", {.returnValue = varSized, .argumentTypes = {varSized}}},
        {"IMAGE_MANIP_ARGMAX_F32", {.returnValue = uint64, .argumentTypes = {varSized}}},
        {"IMAGE_MANIP_MAX_F32", {.returnValue = float32, .argumentTypes = {varSized}}},
        {"IMAGE_MANIP_MAX_ABS_F32", {.returnValue = float32, .argumentTypes = {varSized}}},
    };
    return functions;
}

}

LogicalFunction ImageManipLogicalFunction::create(std::string_view functionName, std::vector<LogicalFunction> children)
{
    const auto function = imageManipFunctions().find(functionName);
    if (function == imageManipFunctions().end())
    {
        throw FunctionNotImplemented("'{}' does not exist", functionName);
    }
    return ImageManipLogicalFunction(function->second.returnValue, std::string(functionName), std::move(children));
}

LogicalFunction ImageManipLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    auto copy = *this;
    for (auto& child : copy.children)
    {
        child = child.withInferredDataType(schema);
    }
    imageManipFunctions().at(copy.functionName).check(copy.functionName, copy.children);
    copy.dataType.nullable = std::ranges::any_of(copy.children, [](const auto& child) { return child.getDataType().nullable; })
        || imageManipFunctions().at(copy.functionName).returnValue.nullable;
    return copy;
}

ImageManipLogicalFunction
Unreflector<ImageManipLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto value = context.unreflect<detail::ReflectedImageManipLogicalFunction>(reflected);
    const auto function = imageManipFunctions().find(value.functionName);
    if (function == imageManipFunctions().end())
    {
        throw FunctionNotImplemented("'{}' does not exist", value.functionName);
    }
    return {function->second.returnValue, std::move(value.functionName), std::move(value.children)};
}

LogicalFunctionRegistryReturnType ImageManipLogicalFunction::createIMAGE_MANIP_MONO8_TO_JPG(LogicalFunctionRegistryArguments arguments)
{
    return create("IMAGE_MANIP_MONO8_TO_JPG", std::move(arguments.children));
}

LogicalFunctionRegistryReturnType ImageManipLogicalFunction::createIMAGE_MANIP_MONO16_TO_MONO8(LogicalFunctionRegistryArguments arguments)
{
    return create("IMAGE_MANIP_MONO16_TO_MONO8", std::move(arguments.children));
}

LogicalFunctionRegistryReturnType ImageManipLogicalFunction::createIMAGE_MANIP_YUYV_TO_JPG(LogicalFunctionRegistryArguments arguments)
{
    return create("IMAGE_MANIP_YUYV_TO_JPG", std::move(arguments.children));
}

LogicalFunctionRegistryReturnType ImageManipLogicalFunction::createIMAGE_MANIP_MONO16_TO_PNG16(LogicalFunctionRegistryArguments arguments)
{
    return create("IMAGE_MANIP_MONO16_TO_PNG16", std::move(arguments.children));
}

LogicalFunctionRegistryReturnType ImageManipLogicalFunction::createIMAGE_MANIP_MONO16_TO_JPG(LogicalFunctionRegistryArguments arguments)
{
    return create("IMAGE_MANIP_MONO16_TO_JPG", std::move(arguments.children));
}

LogicalFunctionRegistryReturnType ImageManipLogicalFunction::createIMAGE_MANIP_MONO16_TO_YUYV(LogicalFunctionRegistryArguments arguments)
{
    return create("IMAGE_MANIP_MONO16_TO_YUYV", std::move(arguments.children));
}

LogicalFunctionRegistryReturnType ImageManipLogicalFunction::createIMAGE_MANIP_MONO8_TO_YUYV(LogicalFunctionRegistryArguments arguments)
{
    return create("IMAGE_MANIP_MONO8_TO_YUYV", std::move(arguments.children));
}

LogicalFunctionRegistryReturnType ImageManipLogicalFunction::createIMAGE_MANIP_FACE_DETECTION(LogicalFunctionRegistryArguments arguments)
{
    return create("IMAGE_MANIP_FACE_DETECTION", std::move(arguments.children));
}

LogicalFunctionRegistryReturnType ImageManipLogicalFunction::createIMAGE_MANIP_SERIALIZE(LogicalFunctionRegistryArguments arguments)
{
    return create("IMAGE_MANIP_SERIALIZE", std::move(arguments.children));
}

LogicalFunctionRegistryReturnType ImageManipLogicalFunction::createIMAGE_MANIP_DESERIALIZE(LogicalFunctionRegistryArguments arguments)
{
    return create("IMAGE_MANIP_DESERIALIZE", std::move(arguments.children));
}

LogicalFunctionRegistryReturnType ImageManipLogicalFunction::createIMAGE_MANIP_DRAW_RECTANGLE(LogicalFunctionRegistryArguments arguments)
{
    return create("IMAGE_MANIP_DRAW_RECTANGLE", std::move(arguments.children));
}

LogicalFunctionRegistryReturnType ImageManipLogicalFunction::createIMAGE_MANIP_MONO16_ROI(LogicalFunctionRegistryArguments arguments)
{
    return create("IMAGE_MANIP_MONO16_ROI", std::move(arguments.children));
}

LogicalFunctionRegistryReturnType ImageManipLogicalFunction::createIMAGE_MANIP_MONO16_AVG(LogicalFunctionRegistryArguments arguments)
{
    return create("IMAGE_MANIP_MONO16_AVG", std::move(arguments.children));
}

LogicalFunctionRegistryReturnType ImageManipLogicalFunction::createIMAGE_MANIP_MONO16_TO_CELSIUS(LogicalFunctionRegistryArguments arguments)
{
    return create("IMAGE_MANIP_MONO16_TO_CELSIUS", std::move(arguments.children));
}

LogicalFunctionRegistryReturnType ImageManipLogicalFunction::createIMAGE_MANIP_MONO16_MAX(LogicalFunctionRegistryArguments arguments)
{
    return create("IMAGE_MANIP_MONO16_MAX", std::move(arguments.children));
}

LogicalFunctionRegistryReturnType ImageManipLogicalFunction::createIMAGE_MANIP_MONO16_MIN(LogicalFunctionRegistryArguments arguments)
{
    return create("IMAGE_MANIP_MONO16_MIN", std::move(arguments.children));
}

LogicalFunctionRegistryReturnType ImageManipLogicalFunction::createIMAGE_MANIP_RECTANGLE(LogicalFunctionRegistryArguments arguments)
{
    return create("IMAGE_MANIP_RECTANGLE", std::move(arguments.children));
}

LogicalFunctionRegistryReturnType ImageManipLogicalFunction::createIMAGE_MANIP_AUDIO_TO_MFCC(LogicalFunctionRegistryArguments arguments)
{
    return create("IMAGE_MANIP_AUDIO_TO_MFCC", std::move(arguments.children));
}

LogicalFunctionRegistryReturnType ImageManipLogicalFunction::createIMAGE_MANIP_ARGMAX_F32(LogicalFunctionRegistryArguments arguments)
{
    return create("IMAGE_MANIP_ARGMAX_F32", std::move(arguments.children));
}

LogicalFunctionRegistryReturnType ImageManipLogicalFunction::createIMAGE_MANIP_MAX_F32(LogicalFunctionRegistryArguments arguments)
{
    return create("IMAGE_MANIP_MAX_F32", std::move(arguments.children));
}

LogicalFunctionRegistryReturnType ImageManipLogicalFunction::createIMAGE_MANIP_MAX_ABS_F32(LogicalFunctionRegistryArguments arguments)
{
    return create("IMAGE_MANIP_MAX_ABS_F32", std::move(arguments.children));
}
}
