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

#include <cstddef>
#include <cstdint>
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
        case DataType::Type::CHAR:
            return true;
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
        std::move(name), std::move(path), std::string{}, std::move(source), std::move(argTypes), {}, returnType, execution, language};
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
    std::optional<std::vector<uint64_t>> bytesArguments;
    std::optional<DataType> returnType;
    std::optional<UdfExecution> execution;
    std::optional<UdfLanguage> language;
    std::optional<std::string> compiledSymbol;
    /// Base64, because the plan is serialized as JSON.
    std::optional<std::string> compiledPipelineBitcode;
    std::optional<std::string> compiledInterpreterBitcode;
    std::optional<std::string> compiler;
};

constexpr std::string_view base64Alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

std::string encodeBase64(const std::string_view data)
{
    std::string encoded;
    encoded.reserve(((data.size() + 2) / 3) * 4);
    for (size_t position = 0; position < data.size(); position += 3)
    {
        const auto remaining = data.size() - position;
        uint32_t chunk = static_cast<uint8_t>(data[position]) << 16U;
        if (remaining > 1)
        {
            chunk |= static_cast<uint8_t>(data[position + 1]) << 8U;
        }
        if (remaining > 2)
        {
            chunk |= static_cast<uint8_t>(data[position + 2]);
        }
        encoded.push_back(base64Alphabet[(chunk >> 18U) & 0x3FU]);
        encoded.push_back(base64Alphabet[(chunk >> 12U) & 0x3FU]);
        encoded.push_back(remaining > 1 ? base64Alphabet[(chunk >> 6U) & 0x3FU] : '=');
        encoded.push_back(remaining > 2 ? base64Alphabet[chunk & 0x3FU] : '=');
    }
    return encoded;
}

std::string decodeBase64(const std::string_view encoded)
{
    if (encoded.size() % 4 != 0)
    {
        throw NES::CannotDeserialize("Failed to deserialize the bitcode of a compiled UDF: invalid base64 length");
    }
    std::string decoded;
    decoded.reserve((encoded.size() / 4) * 3);
    for (size_t position = 0; position < encoded.size(); position += 4)
    {
        uint32_t chunk = 0;
        size_t padding = 0;
        for (size_t offset = 0; offset < 4; ++offset)
        {
            const auto character = encoded[position + offset];
            chunk <<= 6U;
            if (character == '=')
            {
                ++padding;
                continue;
            }
            const auto value = base64Alphabet.find(character);
            if (value == std::string_view::npos)
            {
                throw NES::CannotDeserialize("Failed to deserialize the bitcode of a compiled UDF: invalid base64 character");
            }
            chunk |= static_cast<uint32_t>(value);
        }
        decoded.push_back(static_cast<char>((chunk >> 16U) & 0xFFU));
        if (padding < 2)
        {
            decoded.push_back(static_cast<char>((chunk >> 8U) & 0xFFU));
        }
        if (padding < 1)
        {
            decoded.push_back(static_cast<char>(chunk & 0xFFU));
        }
    }
    return decoded;
}
}

Reflected Reflector<UdfDescriptor>::operator()(const UdfDescriptor& descriptor, const ReflectionContext& context) const
{
    const auto& source = descriptor.getSource();
    const auto& compiled = descriptor.getCompiled();
    return context.reflect(
        detail::ReflectedUdfDescriptor{
            .name = std::make_optional(descriptor.getName()),
            .path = std::make_optional(descriptor.getPath().string()),
            .entrypoint = std::make_optional(descriptor.getEntrypoint()),
            .sourceParameterNames = source.has_value() ? std::make_optional(source->parameterNames) : std::nullopt,
            .sourceBody = source.has_value() ? std::make_optional(source->body) : std::nullopt,
            .argTypes = std::make_optional(descriptor.getArgTypes()),
            .bytesArguments = std::make_optional(descriptor.getBytesArguments()),
            .returnType = std::make_optional(descriptor.getReturnType()),
            .execution = std::make_optional(descriptor.getExecution()),
            .language = std::make_optional(descriptor.getLanguage()),
            .compiledSymbol = compiled.has_value() ? std::make_optional(compiled->symbol) : std::nullopt,
            .compiledPipelineBitcode
            = compiled.has_value() ? std::make_optional(detail::encodeBase64(compiled->pipelineBitcode)) : std::nullopt,
            .compiledInterpreterBitcode
            = compiled.has_value() ? std::make_optional(detail::encodeBase64(compiled->interpreterBitcode)) : std::nullopt,
            .compiler = std::make_optional(descriptor.getCompiler())});
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
    std::optional<CompiledInlineUdf> compiled;
    if (reflected.compiledSymbol.has_value())
    {
        compiled = CompiledInlineUdf{
            .symbol = std::move(reflected.compiledSymbol).value(),
            .pipelineBitcode = detail::decodeBase64(reflected.compiledPipelineBitcode.value_or("")),
            .interpreterBitcode = detail::decodeBase64(reflected.compiledInterpreterBitcode.value_or(""))};
    }
    /// Bypasses catalog validation: the coordinator already validated; the worker trusts the reflected form.
    return UdfDescriptor{
        std::move(reflected.name).value(),
        std::filesystem::path(std::move(reflected.path).value()),
        std::move(reflected.entrypoint).value(),
        std::move(source),
        std::move(reflected.argTypes).value(),
        std::move(reflected.bytesArguments).value_or(std::vector<uint64_t>{}),
        std::move(reflected.returnType).value(),
        reflected.execution.value_or(UdfExecution::InProcess),
        reflected.language.value_or(UdfLanguage::Python),
        std::move(compiled),
        std::move(reflected.compiler).value_or("Codon")};
}

}
