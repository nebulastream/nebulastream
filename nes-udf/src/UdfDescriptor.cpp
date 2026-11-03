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

#include <UdfDescriptor.hpp>

#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Util/Reflection.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

bool isSupportedUdfType(const DataType::Type type)
{
    switch (type)
    {
        case DataType::Type::BOOLEAN:
        case DataType::Type::INT8:
        case DataType::Type::INT16:
        case DataType::Type::INT32:
        case DataType::Type::INT64:
        case DataType::Type::UINT8:
        case DataType::Type::UINT16:
        case DataType::Type::UINT32:
        case DataType::Type::UINT64:
        case DataType::Type::FLOAT32:
        case DataType::Type::FLOAT64:
        case DataType::Type::VARSIZED:
            return true;
        case DataType::Type::CHAR:
        case DataType::Type::UNDEFINED:
            return false;
    }
    return false;
}

std::optional<std::pair<std::string, std::string>> splitEntrypoint(const std::string_view entrypoint)
{
    const auto lastDot = entrypoint.rfind('.');
    if (lastDot == std::string_view::npos || lastDot == 0 || lastDot + 1 == entrypoint.size())
    {
        return std::nullopt;
    }
    return std::make_pair(std::string{entrypoint.substr(0, lastDot)}, std::string{entrypoint.substr(lastDot + 1)});
}

std::optional<UdfLanguage> parseUdfLanguage(const std::string_view language)
{
    if (toUpperCase(language) == "PYTHON")
    {
        return UdfLanguage::Python;
    }
    return std::nullopt;
}

std::optional<UdfExecution> parseUdfExecution(const std::string_view execution)
{
    const auto upperCase = toUpperCase(execution);
    if (upperCase == "INLINE")
    {
        return UdfExecution::Inline;
    }
    if (upperCase == "IN_PROCESS")
    {
        return UdfExecution::InProcess;
    }
    if (upperCase == "OUT_OF_PROCESS")
    {
        return UdfExecution::OutOfProcess;
    }
    return std::nullopt;
}

UdfDescriptor UdfDescriptor::fromSource(
    std::string name,
    std::filesystem::path path,
    InlineUdfSource source,
    std::vector<DataType> argTypes,
    DataType returnType,
    const UdfExecution execution,
    const UdfLanguage language)
{
    return UdfDescriptor{
        std::move(name), std::move(path), std::string{}, std::move(source), std::move(argTypes), returnType, execution, language};
}

namespace detail
{
/// NOLINTNEXTLINE(misc-use-internal-linkage)
struct ReflectedUdfDescriptor
{
    std::optional<std::string> name;
    std::optional<std::string> path;
    std::optional<std::string> entrypoint;
    std::optional<std::vector<std::string>> sourceParameterNames;
    std::optional<std::string> sourceBody;
    std::optional<std::vector<DataType>> argTypes;
    std::optional<DataType> returnType;
    std::optional<UdfExecution> execution;
    std::optional<UdfLanguage> language;
};
}

Reflected Reflector<UdfDescriptor>::operator()(const UdfDescriptor& descriptor, const ReflectionContext& context) const
{
    const auto& source = descriptor.getSource();
    return context.reflect(
        detail::ReflectedUdfDescriptor{
            .name = std::make_optional(descriptor.getName()),
            .path = std::make_optional(descriptor.getPath().string()),
            .entrypoint = std::make_optional(descriptor.getEntrypoint()),
            .sourceParameterNames = source.has_value() ? std::make_optional(source->parameterNames) : std::nullopt,
            .sourceBody = source.has_value() ? std::make_optional(source->body) : std::nullopt,
            .argTypes = std::make_optional(descriptor.getArgTypes()),
            .returnType = std::make_optional(descriptor.getReturnType()),
            .execution = std::make_optional(descriptor.getExecution()),
            .language = std::make_optional(descriptor.getLanguage())});
}

UdfDescriptor Unreflector<UdfDescriptor>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto reflected = context.unreflect<detail::ReflectedUdfDescriptor>(rfl);
    if (!reflected.name.has_value() || !reflected.path.has_value() || !reflected.entrypoint.has_value() || !reflected.argTypes.has_value()
        || !reflected.returnType.has_value())
    {
        throw NES::CannotDeserialize("Failed to deserialize UdfDescriptor");
    }
    std::optional<InlineUdfSource> source;
    if (reflected.sourceBody.has_value())
    {
        source = InlineUdfSource{
            .parameterNames = std::move(reflected.sourceParameterNames).value_or(std::vector<std::string>{}),
            .body = std::move(reflected.sourceBody).value()};
    }
    /// Bypasses catalog validation: the coordinator already validated; the worker trusts the reflected form.
    return UdfDescriptor{
        std::move(reflected.name).value(),
        std::filesystem::path(std::move(reflected.path).value()),
        std::move(reflected.entrypoint).value(),
        std::move(source),
        std::move(reflected.argTypes).value(),
        std::move(reflected.returnType).value(),
        reflected.execution.value_or(UdfExecution::InProcess),
        reflected.language.value_or(UdfLanguage::Python)};
}

}
