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

#include <algorithm>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <openssl/evp.h>

#include <DataTypes/DataType.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>

namespace
{

std::string base64Encode(const std::string& input)
{
    const auto encodedLen = 4 * ((input.size() + 2) / 3);
    std::string out(encodedLen, '\0');
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) OpenSSL EVP API requires unsigned char*
    const auto result = EVP_EncodeBlock(
        reinterpret_cast<unsigned char*>(out.data()), reinterpret_cast<const unsigned char*>(input.data()), static_cast<int>(input.size()));
    if (result < 0)
    {
        throw NES::CannotDeserialize("EVP_EncodeBlock failed");
    }
    return out;
}

std::string base64Decode(const std::string& input)
{
    const auto maxDecodedLen = 3 * input.size() / 4;
    std::string out(maxDecodedLen, '\0');
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) OpenSSL EVP API requires unsigned char*
    const auto actualLen = EVP_DecodeBlock(
        reinterpret_cast<unsigned char*>(out.data()), reinterpret_cast<const unsigned char*>(input.data()), static_cast<int>(input.size()));
    if (actualLen < 0)
    {
        throw NES::CannotDeserialize("EVP_DecodeBlock failed on malformed base64 input");
    }
    /// EVP_DecodeBlock doesn't account for padding — trim trailing null bytes
    auto padding = std::count(input.rbegin(), input.rend(), '=');
    out.resize(static_cast<size_t>(actualLen - padding));
    return out;
}

}

namespace NES
{

namespace detail
{
struct ReflectedModelOutputField
{
    std::string name;
    DataType dataType;
};

struct ReflectedModel
{
    std::optional<std::string> byteCode;
    std::optional<std::string> functionName;
    std::optional<std::vector<size_t>> shape;
    std::optional<std::vector<size_t>> outputShape;
    std::optional<size_t> dims;
    std::optional<size_t> outputDims;
    std::optional<size_t> inputSizeInBytes;
    std::optional<size_t> outputSizeInBytes;
    std::optional<std::vector<DataType>> inputs;
    std::optional<std::vector<ReflectedModelOutputField>> outputs;
};
}

Reflected Reflector<Model>::operator()(const Model& model) const
{
    auto byteCode = model.getByteCode();
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) byte-to-char for base64 encoding
    std::string rawBytes(reinterpret_cast<const char*>(byteCode.data()), byteCode.size());

    std::vector<detail::ReflectedModelOutputField> outputFields;
    outputFields.reserve(model.getOutputs().size());
    for (const auto& [name, dt] : model.getOutputs())
    {
        outputFields.push_back({name, dt});
    }

    return reflect(detail::ReflectedModel{
        .byteCode = std::make_optional(base64Encode(rawBytes)),
        .functionName = std::make_optional(model.getFunctionName()),
        .shape = std::make_optional(model.getInputShape()),
        .outputShape = std::make_optional(model.getOutputShape()),
        .dims = std::make_optional(model.getNDim()),
        .outputDims = std::make_optional(model.getOutputDims()),
        .inputSizeInBytes = std::make_optional(model.inputSize()),
        .outputSizeInBytes = std::make_optional(model.outputSize()),
        .inputs = std::make_optional(std::vector<DataType>(model.getInputs().begin(), model.getInputs().end())),
        .outputs = std::make_optional(std::move(outputFields))});
}

Model Unreflector<Model>::operator()(const Reflected& rfl) const
{
    auto reflected = unreflect<detail::ReflectedModel>(rfl);

    Model model;
    if (reflected.byteCode.has_value())
    {
        auto decoded = base64Decode(reflected.byteCode.value());
        /// NOLINTNEXTLINE(modernize-avoid-c-arrays) dynamic byte buffer requires array form
        auto buffer = std::make_shared<std::byte[]>(decoded.size());
        std::ranges::transform(decoded, buffer.get(), [](char c) { return static_cast<std::byte>(c); });
        model.byteCode = {std::move(buffer), decoded.size()};
    }
    model.functionName = reflected.functionName.value_or("");
    model.shape = reflected.shape.value_or(std::vector<size_t>{});
    model.outputShape = reflected.outputShape.value_or(std::vector<size_t>{});
    model.dims = reflected.dims.value_or(0);
    model.outputDims = reflected.outputDims.value_or(0);
    model.inputSizeInBytes = reflected.inputSizeInBytes.value_or(0);
    model.outputSizeInBytes = reflected.outputSizeInBytes.value_or(0);
    model.inputs = reflected.inputs.value_or(std::vector<DataType>{});

    std::vector<std::pair<std::string, DataType>> outputs;
    if (reflected.outputs.has_value())
    {
        for (auto& field : reflected.outputs.value())
        {
            outputs.emplace_back(std::move(field.name), field.dataType);
        }
    }
    model.outputs = std::move(outputs);

    return model;
}

}
