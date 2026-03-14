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

#include <Operators/InferModelNameLogicalOperator.hpp>

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
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{

InferModelNameLogicalOperator::InferModelNameLogicalOperator(std::string modelName, std::vector<std::string> inputFieldNames)
    : modelName(std::move(modelName)), inputFieldNames(std::move(inputFieldNames))
{
}

std::string_view InferModelNameLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string InferModelNameLogicalOperator::getModelName() const
{
    return modelName;
}

std::vector<std::string> InferModelNameLogicalOperator::getInputFieldNames() const
{
    return inputFieldNames;
}

bool InferModelNameLogicalOperator::operator==(const InferModelNameLogicalOperator& rhs) const
{
    return modelName == rhs.modelName && inputFieldNames == rhs.inputFieldNames && getOutputSchema() == rhs.getOutputSchema()
        && getInputSchemas() == rhs.getInputSchemas() && getTraitSet() == rhs.getTraitSet();
}

std::string InferModelNameLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "INFER_MODEL_NAME(opId: {}, model: {}, inputFields: [{}], traitSet: {})",
            opId,
            modelName,
            fmt::join(inputFieldNames, ", "),
            traitSet.explain(verbosity));
    }
    return fmt::format("INFER_MODEL_NAME(model: {}, inputFields: [{}])", modelName, fmt::join(inputFieldNames, ", "));
}

InferModelNameLogicalOperator InferModelNameLogicalOperator::withInferredSchema([[maybe_unused]] std::vector<Schema> inputSchemas) const
{
    throw CannotInferSchema("InferModelName requires model resolution before schema inference");
}

TraitSet InferModelNameLogicalOperator::getTraitSet() const
{
    return traitSet;
}

InferModelNameLogicalOperator InferModelNameLogicalOperator::withTraitSet(TraitSet newTraitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(newTraitSet);
    return copy;
}

InferModelNameLogicalOperator InferModelNameLogicalOperator::withChildren(std::vector<LogicalOperator> newChildren) const
{
    auto copy = *this;
    copy.children = std::move(newChildren);
    return copy;
}

std::vector<Schema> InferModelNameLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
}

Schema InferModelNameLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<LogicalOperator> InferModelNameLogicalOperator::getChildren() const
{
    return children;
}

Reflected Reflector<InferModelNameLogicalOperator>::operator()(const InferModelNameLogicalOperator& op) const
{
    return reflect(
        detail::ReflectedInferModelNameLogicalOperator{std::make_optional(op.getModelName()), std::make_optional(op.getInputFieldNames())});
}

InferModelNameLogicalOperator Unreflector<InferModelNameLogicalOperator>::operator()(const Reflected& rfl) const
{
    auto [modelNameOpt, inputFieldNamesOpt] = unreflect<detail::ReflectedInferModelNameLogicalOperator>(rfl);

    if (!modelNameOpt.has_value() || !inputFieldNamesOpt.has_value())
    {
        throw CannotDeserialize("Failed to deserialize InferModelNameLogicalOperator");
    }

    return InferModelNameLogicalOperator(modelNameOpt.value(), inputFieldNamesOpt.value());
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterInferModelNameLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<InferModelNameLogicalOperator>(arguments.reflected);
    }
    PRECONDITION(false, "Operator is only built directly or via reflection, not using the registry");
    std::unreachable();
}
}
