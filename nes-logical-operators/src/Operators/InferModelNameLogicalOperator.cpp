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

namespace NES
{

InferModelNameLogicalOperator::InferModelNameLogicalOperator(std::string modelName, std::vector<LogicalFunction> inputFields)
    : modelName(std::move(modelName)), inputFields(std::move(inputFields))
{
}

std::string_view InferModelNameLogicalOperator::getName() const noexcept
{
    return NAME;
}

const std::string& InferModelNameLogicalOperator::getModelName() const
{
    return modelName;
}

const std::vector<LogicalFunction>& InferModelNameLogicalOperator::getInputFields() const
{
    return inputFields;
}

bool InferModelNameLogicalOperator::operator==(const InferModelNameLogicalOperator& rhs) const
{
    return modelName == rhs.modelName && inputFields == rhs.inputFields;
}

std::string InferModelNameLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("INFER_MODEL_NAME(opId: {}, model: {})", opId, modelName);
    }
    return fmt::format("INFER_MODEL_NAME({})", modelName);
}

InferModelNameLogicalOperator InferModelNameLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    if (!inputSchemas.empty())
    {
        copy.inputSchema = inputSchemas.at(0);
        // Output schema is the same as input - it will be modified when resolved to InferModelLogicalOperator
        copy.outputSchema = inputSchemas.at(0);
    }
    return copy;
}

TraitSet InferModelNameLogicalOperator::getTraitSet() const
{
    return traitSet;
}

InferModelNameLogicalOperator InferModelNameLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

InferModelNameLogicalOperator InferModelNameLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = std::move(children);
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
    return reflect(detail::ReflectedInferModelNameLogicalOperator{op.getModelName(), op.getInputFields()});
}

InferModelNameLogicalOperator Unreflector<InferModelNameLogicalOperator>::operator()(const Reflected& rfl) const
{
    auto [modelName, inputFields] = unreflect<detail::ReflectedInferModelNameLogicalOperator>(rfl);
    return InferModelNameLogicalOperator(std::move(modelName), std::move(inputFields));
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterInferModelNameLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<InferModelNameLogicalOperator>(arguments.reflected);
    }
    PRECONDITION(false, "Operator is only built directly via parser or via reflection, not using the registry");
    std::unreachable();
}
}
