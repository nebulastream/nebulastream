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

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <ModelCatalog.hpp>

namespace NES
{

InferModelLogicalOperator::InferModelLogicalOperator(RegisteredModel model, std::vector<std::string> inputFieldNames)
    : model(std::move(model)), inputFieldNames(std::move(inputFieldNames))
{
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
std::string_view InferModelLogicalOperator::getName() const noexcept
{
    return NAME;
}

const RegisteredModel& InferModelLogicalOperator::getModel() const
{
    return model;
}

std::vector<std::string> InferModelLogicalOperator::getInputFieldNames() const
{
    return inputFieldNames;
}

std::vector<std::string> InferModelLogicalOperator::getOutputFieldNames() const
{
    return model.getSchema().outputs.getFieldNames();
}

bool InferModelLogicalOperator::hasVarsizedInput() const
{
    const auto& inputs = model.getSchema().inputs;
    return inputs.getNumberOfFields() > 0 && inputs.getFieldAt(0).dataType.isType(DataType::Type::VARSIZED);
}

bool InferModelLogicalOperator::hasVarsizedOutput() const
{
    const auto& outputs = model.getSchema().outputs;
    return outputs.getNumberOfFields() > 0 && outputs.getFieldAt(0).dataType.isType(DataType::Type::VARSIZED);
}

bool InferModelLogicalOperator::operator==(const InferModelLogicalOperator& rhs) const
{
    return model == rhs.model && inputFieldNames == rhs.inputFieldNames && getOutputSchema() == rhs.getOutputSchema()
        && getInputSchemas() == rhs.getInputSchemas() && getTraitSet() == rhs.getTraitSet();
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
std::string InferModelLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "INFER_MODEL(opId: {}, inputFields: [{}], traitSet: {})", opId, fmt::join(inputFieldNames, ", "), traitSet.explain(verbosity));
    }
    return fmt::format("INFER_MODEL(inputFields: [{}])", fmt::join(inputFieldNames, ", "));
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
InferModelLogicalOperator InferModelLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    if (inputSchemas.empty())
    {
        throw CannotInferSchema("InferModel requires at least one input schema");
    }
    copy.inputSchema = inputSchemas.at(0);

    const auto& modelInputs = model.getSchema().inputs;
    const auto& modelOutputs = model.getSchema().outputs;

    /// Check input field count matches model inputs
    if (inputFieldNames.size() != modelInputs.getNumberOfFields())
    {
        throw CannotInferSchema(
            "Model expects {} inputs, but {} input field names were provided", modelInputs.getNumberOfFields(), inputFieldNames.size());
    }

    /// Check type compatibility for each input field and resolve to its fully qualified
    /// name (`source$field`) so the runtime record lookup, which matches strictly, can find it.
    for (size_t i = 0; i < inputFieldNames.size(); ++i)
    {
        const auto& fieldName = inputFieldNames[i];
        const auto field = copy.inputSchema.getFieldByName(fieldName);
        if (!field.has_value())
        {
            throw CannotInferSchema("Field '{}' not found in input schema", fieldName);
        }
        if (field->dataType.nullable)
        {
            throw CannotInferSchema("Field '{}' is nullable, but model inputs must not be nullable", fieldName);
        }
        if (field->dataType.type != modelInputs.getFieldAt(i).dataType.type)
        {
            throw CannotInferSchema("Type mismatch for field '{}': schema has a different type than model expects", fieldName);
        }
        copy.inputFieldNames[i] = field->name;
    }

    /// Build output schema: start from input schema, then append/replace model output fields
    copy.outputSchema = copy.inputSchema;
    for (const auto& field : modelOutputs.getFields())
    {
        if (copy.outputSchema.getFieldByName(field.name).has_value())
        {
            /// Field already exists — replace its type in-place
            [[maybe_unused]] const bool replaced = copy.outputSchema.replaceTypeOfField(field.name, field.dataType);
        }
        else
        {
            copy.outputSchema = copy.outputSchema.addField(field.name, field.dataType);
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

/// generated registry interface requires by-value argument
LogicalOperatorRegistryReturnType
/// NOLINTNEXTLINE(performance-unnecessary-value-param)
LogicalOperatorGeneratedRegistrar::RegisterInferModelLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return ReflectionContext{}.unreflect<InferModelLogicalOperator>(arguments.reflected);
    }
    PRECONDITION(false, "Operator is only built directly or via reflection, not using the registry");
    std::unreachable();
}
}
