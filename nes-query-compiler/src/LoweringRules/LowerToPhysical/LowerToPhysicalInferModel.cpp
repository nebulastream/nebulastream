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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalInferModel.hpp>

#include <memory>
#include <ranges>
#include <Inference/IREEInferenceOperatorHandler.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/InferModelLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <InferModelPhysicalOperator.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalInferModel::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGetAs<InferModelLogicalOperator>(), "Expected an InferModelLogicalOperator");
    const auto inferModelOp = logicalOperator.getAs<InferModelLogicalOperator>();
    auto model = inferModelOp.get().getModel();

    NES_DEBUG("Lowering InferModel operator to physical IREE operator (function: {})", model.getFunctionName());

    const auto inputSchema = logicalOperator.getInputSchemas().at(0);

    /// Extract input field names from the logical operator, resolving to qualified schema names.
    /// If a model input maps to a VARSIZED schema field, use bulk byte input mode.
    std::vector<std::string> inputFields;
    bool varsizedInput = false;
    for (const auto& name : inferModelOp.get().getInputFieldNames())
    {
        auto field = inputSchema.getFieldByName(name);
        if (!field.has_value())
        {
            throw InferenceRuntime("Model input field '{}' not found in schema", name);
        }
        inputFields.push_back(field->name);
        if (field->dataType.type == DataType::Type::VARSIZED)
        {
            varsizedInput = true;
        }
    }

    /// Extract output field names from model
    auto outputFields
        = std::views::transform(model.getOutputs(), [](const auto& pair) { return pair.first; }) | std::ranges::to<std::vector>();

    /// Create the operator handler
    auto handlerId = getNextOperatorHandlerId();
    auto handler = std::make_shared<Inference::IREEInferenceOperatorHandler>(model);

    /// Create the physical operator
    auto physicalOperator = InferModelPhysicalOperator(handlerId, inputFields, outputFields, varsizedInput);

    const auto memoryLayoutTypeTrait = logicalOperator.getTraitSet().tryGet<MemoryLayoutTypeTrait>();
    PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
    const auto memoryLayoutType = memoryLayoutTypeTrait.value()->memoryLayout;

    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator,
        logicalOperator.getInputSchemas()[0],
        logicalOperator.getOutputSchema(),
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    std::vector leafes(logicalOperator.getChildren().size(), wrapper);
    return {.root = wrapper, .leafs = {leafes}};
}

std::unique_ptr<AbstractLoweringRule>
LoweringRuleGeneratedRegistrar::RegisterInferModelLoweringRule(LoweringRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalInferModel>(argument.conf);
}

}
