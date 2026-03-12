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
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <Util/Reflection.hpp>
#include <Model.hpp>

namespace NES
{

InferModelLogicalOperator::InferModelLogicalOperator(Nebuli::Inference::Model model, std::vector<std::string> inputFieldNames)
    : model(std::move(model)), inputFieldNames(std::move(inputFieldNames))
{
}

std::string_view InferModelLogicalOperator::getName() const noexcept
{
    return NAME;
}

Nebuli::Inference::Model InferModelLogicalOperator::getModel() const
{
    return model;
}

std::vector<std::string> InferModelLogicalOperator::getInputFieldNames() const
{
    return inputFieldNames;
}

bool InferModelLogicalOperator::operator==(const InferModelLogicalOperator& rhs) const
{
    return model == rhs.model && inputFieldNames == rhs.inputFieldNames && getOutputSchema() == rhs.getOutputSchema()
        && getInputSchemas() == rhs.getInputSchemas() && getTraitSet() == rhs.getTraitSet();
}

std::string InferModelLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "INFER_MODEL(opId: {}, inputFields: [{}], traitSet: {})",
            opId,
            fmt::join(inputFieldNames, ", "),
            traitSet.explain(verbosity));
    }
    return fmt::format("INFER_MODEL(inputFields: [{}])", fmt::join(inputFieldNames, ", "));
}

InferModelLogicalOperator InferModelLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    if (inputSchemas.empty())
    {
        throw CannotInferSchema("InferModel requires at least one input schema");
    }
    copy.inputSchema = inputSchemas.at(0);

    // Check input field count matches model inputs
    if (inputFieldNames.size() != model.getInputs().size())
    {
        throw CannotInferSchema(
            "Model expects {} inputs, but {} input field names were provided",
            model.getInputs().size(),
            inputFieldNames.size());
    }

    // Check type compatibility for each input field
    for (size_t i = 0; i < inputFieldNames.size(); ++i)
    {
        const auto& fieldName = inputFieldNames[i];
        const auto field = copy.inputSchema.getFieldByName(fieldName);
        if (!field.has_value())
        {
            throw CannotInferSchema("Field '{}' not found in input schema", fieldName);
        }
        if (field->dataType != model.getInputs()[i])
        {
            throw CannotInferSchema(
                "Type mismatch for field '{}': schema has a different type than model expects", fieldName);
        }
    }

    // Build output schema: start from input schema, then append/replace model output fields
    copy.outputSchema = copy.inputSchema;
    for (const auto& [name, dataType] : model.getOutputs())
    {
        if (copy.outputSchema.getFieldByName(name).has_value())
        {
            // Field already exists — replace its type in-place
            [[maybe_unused]] bool replaced = copy.outputSchema.replaceTypeOfField(name, dataType);
        }
        else
        {
            copy.outputSchema = copy.outputSchema.addField(name, dataType);
        }
    }
    return copy;
}

TraitSet InferModelLogicalOperator::getTraitSet() const
{
    return traitSet;
}

InferModelLogicalOperator InferModelLogicalOperator::withTraitSet(TraitSet newTraitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(newTraitSet);
    return copy;
}

InferModelLogicalOperator InferModelLogicalOperator::withChildren(std::vector<LogicalOperator> newChildren) const
{
    auto copy = *this;
    copy.children = std::move(newChildren);
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
    // Serialize model to protobuf bytes for reflection
    SerializableOperator_Model proto;
    Nebuli::Inference::serializeModel(op.getModel(), proto);
    std::string bytes;
    proto.SerializeToString(&bytes);

    return reflect(detail::ReflectedInferModelLogicalOperator{
        std::make_optional(std::move(bytes)),
        std::make_optional(op.getInputFieldNames())});
}

InferModelLogicalOperator Unreflector<InferModelLogicalOperator>::operator()(const Reflected& rfl) const
{
    auto [modelBytesOpt, inputFieldNamesOpt] = unreflect<detail::ReflectedInferModelLogicalOperator>(rfl);

    if (!modelBytesOpt.has_value() || !inputFieldNamesOpt.has_value())
    {
        throw CannotDeserialize("Failed to deserialize InferModelLogicalOperator");
    }

    SerializableOperator_Model proto;
    if (!proto.ParseFromString(modelBytesOpt.value()))
    {
        throw CannotDeserialize("Failed to parse InferModelLogicalOperator model proto");
    }

    return InferModelLogicalOperator(Nebuli::Inference::deserializeModel(proto), inputFieldNamesOpt.value());
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterInferModelLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<InferModelLogicalOperator>(arguments.reflected);
    }
    PRECONDITION(false, "Operator is only built directly or via reflection, not using the registry");
    std::unreachable();
}
}
