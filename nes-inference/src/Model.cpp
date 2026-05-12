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

#include <Model.hpp>

#include <cstddef>
#include <filesystem>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <openssl/evp.h>

#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <ModelCatalog.hpp>

namespace NES
{

namespace detail
{
struct ReflectedImportedModel
{
    std::optional<std::string> payload;
    std::optional<std::string> auxiliary;
    std::optional<std::string> functionName;
    std::optional<std::vector<size_t>> inputShape;
    std::optional<std::vector<size_t>> outputShape;
};

struct
    ReflectedRegisteredModel /// NOLINT(bugprone-exception-escape) defaulted special members on a struct holding optionals of vector-backed types trip the check; no real escape
{
    std::optional<std::string> name;
    std::optional<std::string> path;
    std::optional<Reflected> imported;
    std::optional<ModelFieldList> inputs;
    std::optional<ModelFieldList> outputs;
};
}

namespace
{

std::string bytesToBase64(std::span<const std::byte> data)
{
    if (data.empty())
    {
        return {};
    }
    constexpr auto maxEncodableInputSize = static_cast<size_t>(std::numeric_limits<int>::max() / 4) * 3;
    if (data.size() > maxEncodableInputSize)
    {
        throw CannotSerialize("Model payload is too large for OpenSSL base64 encoding");
    }

    std::string encoded(((data.size() + 2) / 3) * 4, '\0');
    const auto encodedLen = EVP_EncodeBlock(
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) OpenSSL EVP API requires unsigned char*
        reinterpret_cast<unsigned char*>(encoded.data()),
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) OpenSSL EVP API requires unsigned char*
        reinterpret_cast<const unsigned char*>(data.data()),
        static_cast<int>(data.size()));
    encoded.resize(static_cast<size_t>(encodedLen));
    return encoded;
}

detail::RefCountedByteBuffer base64ToBytes(std::string_view data)
{
    if (data.empty())
    {
        return {};
    }
    if (data.size() % 4 != 0)
    {
        throw CannotDeserialize("Invalid base64 model payload length");
    }
    if (data.size() > static_cast<size_t>(std::numeric_limits<int>::max()))
    {
        throw CannotDeserialize("Base64 model payload is too large for OpenSSL decoding");
    }

    const auto thirdIsPadding = data[data.size() - 2] == '=';
    const auto fourthIsPadding = data[data.size() - 1] == '=';
    if (thirdIsPadding && !fourthIsPadding)
    {
        throw CannotDeserialize("Invalid base64 model payload padding");
    }

    std::vector<std::byte> decoded((data.size() / 4) * 3);
    const auto decodedLen = EVP_DecodeBlock(
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) OpenSSL EVP API requires unsigned char*
        reinterpret_cast<unsigned char*>(decoded.data()),
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) OpenSSL EVP API requires unsigned char*
        reinterpret_cast<const unsigned char*>(data.data()),
        static_cast<int>(data.size()));
    if (decodedLen < 0)
    {
        throw CannotDeserialize("Invalid base64 model payload character");
    }

    const auto padding = static_cast<size_t>(thirdIsPadding) + static_cast<size_t>(fourthIsPadding);
    decoded.resize(static_cast<size_t>(decodedLen) - padding);
    return detail::RefCountedByteBuffer::fromBytes(decoded);
}

}

Reflected Reflector<ImportedModel>::operator()(const ImportedModel& model, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedImportedModel{
        .payload = std::make_optional(bytesToBase64(model.getData())),
        .auxiliary = std::make_optional(bytesToBase64(model.getAuxiliaryData())),
        .functionName = std::make_optional(model.getFunctionName()),
        .inputShape = std::make_optional(model.getInputShape()),
        .outputShape = std::make_optional(model.getOutputShape())});
}

ImportedModel Unreflector<ImportedModel>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto reflected = context.unreflect<detail::ReflectedImportedModel>(rfl);
    return ImportedModel{
        base64ToBytes(reflected.payload.value_or(std::string{})),
        base64ToBytes(reflected.auxiliary.value_or(std::string{})),
        reflected.functionName.value_or(std::string{}),
        reflected.inputShape.value_or(std::vector<size_t>{}),
        reflected.outputShape.value_or(std::vector<size_t>{})};
}

Reflected Reflector<RegisteredModel>::operator()(const RegisteredModel& model, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedRegisteredModel{
        .name = std::make_optional(model.getName()),
        .path = std::make_optional(model.getPath().string()),
        .imported = std::make_optional(Reflector<ImportedModel>{}(model.getImported(), context)),
        .inputs = std::make_optional(model.getSchema().inputs),
        .outputs = std::make_optional(model.getSchema().outputs)});
}

RegisteredModel Unreflector<RegisteredModel>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto reflected = context.unreflect<detail::ReflectedRegisteredModel>(rfl);
    if (!reflected.name.has_value() || !reflected.path.has_value() || !reflected.imported.has_value() || !reflected.inputs.has_value()
        || !reflected.outputs.has_value())
    {
        throw NES::CannotDeserialize("Failed to deserialize RegisteredModel");
    }
    /// Bypasses catalog validation: the coordinator already validated; the worker trusts the reflected form.
    /// Schema's user-declared destructor suppresses its implicit move ctor; std::move on the field initializers
    /// would just rebind to copy-from-const-ref. Pass by value.
    ModelSchema schema{.inputs = reflected.inputs.value(), .outputs = reflected.outputs.value()};
    return RegisteredModel{
        std::move(reflected.name).value(),
        std::filesystem::path(std::move(reflected.path).value()),
        context.unreflect<ImportedModel>(reflected.imported.value()),
        std::move(schema)};
}

}
