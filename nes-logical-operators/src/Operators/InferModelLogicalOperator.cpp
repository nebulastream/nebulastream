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

#include <Operators/InferModelLogicalOperator.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace
{
/// Base64 encoding table
constexpr std::string_view BASE64_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

std::string base64Encode(std::span<const std::byte> data)
{
    std::string result;
    result.reserve((data.size() + 2) / 3 * 4);
    for (size_t i = 0; i < data.size(); i += 3)
    {
        uint32_t n = static_cast<uint8_t>(data[i]) << 16;
        if (i + 1 < data.size()) n |= static_cast<uint8_t>(data[i + 1]) << 8;
        if (i + 2 < data.size()) n |= static_cast<uint8_t>(data[i + 2]);
        result += BASE64_CHARS[(n >> 18) & 0x3F];
        result += BASE64_CHARS[(n >> 12) & 0x3F];
        result += (i + 1 < data.size()) ? BASE64_CHARS[(n >> 6) & 0x3F] : '=';
        result += (i + 2 < data.size()) ? BASE64_CHARS[n & 0x3F] : '=';
    }
    return result;
}

std::pair<std::shared_ptr<std::byte[]>, size_t> base64Decode(std::string_view encoded)
{
    /// Build reverse lookup
    std::array<uint8_t, 256> lookup{};
    lookup.fill(0xFF);
    for (size_t i = 0; i < BASE64_CHARS.size(); ++i)
        lookup[static_cast<uint8_t>(BASE64_CHARS[i])] = static_cast<uint8_t>(i);

    size_t outSize = encoded.size() / 4 * 3;
    if (!encoded.empty() && encoded.back() == '=') --outSize;
    if (encoded.size() >= 2 && encoded[encoded.size() - 2] == '=') --outSize;

    auto buffer = std::make_shared<std::byte[]>(outSize);
    size_t j = 0;
    for (size_t i = 0; i < encoded.size(); i += 4)
    {
        uint32_t n = lookup[static_cast<uint8_t>(encoded[i])] << 18;
        n |= lookup[static_cast<uint8_t>(encoded[i + 1])] << 12;
        if (encoded[i + 2] != '=') n |= lookup[static_cast<uint8_t>(encoded[i + 2])] << 6;
        if (encoded[i + 3] != '=') n |= lookup[static_cast<uint8_t>(encoded[i + 3])];
        buffer[j++] = static_cast<std::byte>((n >> 16) & 0xFF);
        if (encoded[i + 2] != '=') buffer[j++] = static_cast<std::byte>((n >> 8) & 0xFF);
        if (encoded[i + 3] != '=') buffer[j++] = static_cast<std::byte>(n & 0xFF);
    }
    return {buffer, outSize};
}
}

namespace NES
{

InferModelLogicalOperator::InferModelLogicalOperator(Inference::Model model, std::vector<LogicalFunction> inputFields)
    : model(std::move(model)), inputFields(std::move(inputFields))
{
}

std::string_view InferModelLogicalOperator::getName() const noexcept
{
    return NAME;
}

const Inference::Model& InferModelLogicalOperator::getModel() const
{
    return model;
}

Inference::Model& InferModelLogicalOperator::getModel()
{
    return model;
}

const std::vector<LogicalFunction>& InferModelLogicalOperator::getInputFields() const
{
    return inputFields;
}

bool InferModelLogicalOperator::operator==(const InferModelLogicalOperator& rhs) const
{
    return model == rhs.model && inputFields == rhs.inputFields;
}

std::string InferModelLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("INFER_MODEL(opId: {}, function: {})", opId, model.getFunctionName());
    }
    return fmt::format("INFER_MODEL(function: {})", model.getFunctionName());
}

InferModelLogicalOperator InferModelLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    if (!inputSchemas.empty())
    {
        copy.inputSchema = inputSchemas.at(0);
        // Build output schema: input schema + model output fields
        auto outSchema = inputSchemas.at(0);
        for (const auto& [fieldName, dataType] : copy.model.getOutputs())
        {
            outSchema = outSchema.addField(fieldName, *dataType);
        }
        copy.outputSchema = outSchema;
    }
    return copy;
}

TraitSet InferModelLogicalOperator::getTraitSet() const
{
    return traitSet;
}

InferModelLogicalOperator InferModelLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

InferModelLogicalOperator InferModelLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = std::move(children);
    return copy;
}

std::vector<Schema> InferModelLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
}

Schema InferModelLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<LogicalOperator> InferModelLogicalOperator::getChildren() const
{
    return children;
}

Reflected Reflector<InferModelLogicalOperator>::operator()(const InferModelLogicalOperator& op) const
{
    const auto& m = op.getModel();
    std::vector<detail::ReflectedModelOutput> modelOutputs;
    for (const auto& [name, dt] : m.getOutputs())
    {
        modelOutputs.push_back({name, dt->type});
    }
    std::vector<detail::ReflectedModelOutput> modelInputs;
    for (const auto& dt : m.getInputs())
    {
        modelInputs.push_back({"", dt->type});
    }
    return reflect(detail::ReflectedInferModelLogicalOperator{
        .functionName = m.getFunctionName(),
        .inputFields = op.getInputFields(),
        .modelOutputs = std::move(modelOutputs),
        .modelInputs = std::move(modelInputs),
        .bytecodeBase64 = base64Encode(m.getByteCode()),
        .inputShape = m.getInputShape(),
        .outputShape = m.getOutputShape(),
        .inputSizeInBytes = m.inputSize(),
        .outputSizeInBytes = m.outputSize(),
    });
}

InferModelLogicalOperator Unreflector<InferModelLogicalOperator>::operator()(const Reflected& rfl) const
{
    auto [functionName, inputFields, modelOutputs, modelInputs, bytecodeBase64, inputShape, outputShape, inputSizeInBytes, outputSizeInBytes]
        = unreflect<detail::ReflectedInferModelLogicalOperator>(rfl);

    auto [byteCode, byteCodeSize] = base64Decode(bytecodeBase64);
    Inference::Model model{std::move(byteCode), byteCodeSize};
    model.setFunctionName(std::move(functionName));
    model.setInputShape(std::move(inputShape));
    model.setOutputShape(std::move(outputShape));
    model.setInputSizeInBytes(inputSizeInBytes);
    model.setOutputSizeInBytes(outputSizeInBytes);

    std::vector<std::shared_ptr<DataType>> inputs;
    for (auto& [name, type] : modelInputs)
    {
        inputs.emplace_back(std::make_shared<DataType>(type));
    }
    model.setInputs(std::move(inputs));

    std::vector<std::pair<std::string, std::shared_ptr<DataType>>> outputs;
    for (auto& [name, type] : modelOutputs)
    {
        outputs.emplace_back(std::move(name), std::make_shared<DataType>(type));
    }
    model.setOutputs(std::move(outputs));

    return InferModelLogicalOperator(std::move(model), std::move(inputFields));
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterInferModelLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<InferModelLogicalOperator>(arguments.reflected);
    }
    PRECONDITION(false, "Operator is only built directly via parser or via reflection, not using the registry");
    std::unreachable();
}
}
