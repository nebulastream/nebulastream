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
#include <cstddef>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <openssl/evp.h>

#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace
{
std::string base64Encode(std::span<const std::byte> data)
{
    const auto maxLen = 4 * ((data.size() + 2) / 3) + 1;
    std::string result(maxLen, '\0');
    const auto len = EVP_EncodeBlock(
        reinterpret_cast<unsigned char*>(result.data()),
        reinterpret_cast<const unsigned char*>(data.data()),
        static_cast<int>(data.size()));
    result.resize(static_cast<size_t>(len));
    return result;
}

std::pair<std::shared_ptr<std::byte[]>, size_t> base64Decode(std::string_view encoded)
{
    const auto maxLen = 3 * encoded.size() / 4;
    auto buffer = std::make_shared<std::byte[]>(maxLen);
    auto decodedLen = EVP_DecodeBlock(
        reinterpret_cast<unsigned char*>(buffer.get()),
        reinterpret_cast<const unsigned char*>(encoded.data()),
        static_cast<int>(encoded.size()));
    if (decodedLen < 0)
    {
        return {nullptr, 0};
    }
    /// EVP_DecodeBlock doesn't account for padding
    auto actualLen = static_cast<size_t>(decodedLen);
    if (!encoded.empty() && encoded.back() == '=') --actualLen;
    if (encoded.size() >= 2 && encoded[encoded.size() - 2] == '=') --actualLen;
    return {buffer, actualLen};
}
}

namespace NES
{

InferModelLogicalOperator::InferModelLogicalOperator(Inference::Model model)
    : model(std::move(model))
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

std::vector<std::string> InferModelLogicalOperator::getInputFieldNames() const
{
    std::vector<std::string> names;
    for (const auto& [name, _] : model.getInputs())
    {
        names.push_back(name);
    }
    return names;
}

bool InferModelLogicalOperator::operator==(const InferModelLogicalOperator& rhs) const
{
    return model == rhs.model;
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
    for (const auto& [name, dt] : m.getInputs())
    {
        modelInputs.push_back({name, dt->type});
    }
    return reflect(detail::ReflectedInferModelLogicalOperator{
        .functionName = m.getFunctionName(),
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
    auto [functionName, modelOutputs, modelInputs, bytecodeBase64, inputShape, outputShape, inputSizeInBytes, outputSizeInBytes]
        = unreflect<detail::ReflectedInferModelLogicalOperator>(rfl);

    auto [byteCode, byteCodeSize] = base64Decode(bytecodeBase64);
    Inference::Model model{std::move(byteCode), byteCodeSize};
    model.setFunctionName(std::move(functionName));
    model.setInputShape(std::move(inputShape));
    model.setOutputShape(std::move(outputShape));
    model.setInputSizeInBytes(inputSizeInBytes);
    model.setOutputSizeInBytes(outputSizeInBytes);

    std::vector<std::pair<std::string, std::shared_ptr<DataType>>> inputs;
    for (auto& [name, type] : modelInputs)
    {
        inputs.emplace_back(std::move(name), std::make_shared<DataType>(type));
    }
    model.setInputs(std::move(inputs));

    std::vector<std::pair<std::string, std::shared_ptr<DataType>>> outputs;
    for (auto& [name, type] : modelOutputs)
    {
        outputs.emplace_back(std::move(name), std::make_shared<DataType>(type));
    }
    model.setOutputs(std::move(outputs));

    return InferModelLogicalOperator(std::move(model));
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
