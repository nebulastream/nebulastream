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
#include <utility>
#include <vector>

#include <ranges>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/InferModelLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/SchemaFactory.hpp>
#include <ErrorHandling.hpp>
#include <InferModelPhysicalOperator.hpp>
#include <Inference.hpp>
#include <LoweringRuleRegistry.hpp>
#include <Model.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalInferModel::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGetAs<InferModelLogicalOperator>(), "Expected an InferModelLogicalOperator");
    const auto inferModelOp = logicalOperator.getAs<InferModelLogicalOperator>();

    /// Move the imported model to its executable form. Deliberately deferred to
    /// lowering so the coordinator only ships the imported IR across the wire. The
    /// schema and signature travel with the `ImportedModel` and are propagated by
    /// `compileModel`, so no extra wiring is needed here.
    auto compiled = compileModel(inferModelOp.get().getModel().getImported());
    if (!compiled)
    {
        throw CannotLoadModel("Failed to compile model during lowering: {}", compiled.error().message);
    }
    auto model = std::move(*compiled);

    /// Create the physical operator. Both input and output field names come from the model's
    /// declared schema. The runtime `record.read` lookup matches by canonical identifier text,
    /// and `withInferredSchema` already verified the child carries fields with these names.
    /// The operator owns its own thread-local runtime pool; no OperatorHandler needed.
    const auto toIdList = [](const auto& fields)
    {
        return fields
            | std::views::transform([](const UnqualifiedUnboundField& field)
                                    { return static_cast<QualifiedIdentifier>(field.getFullyQualifiedName()); })
            | std::ranges::to<std::vector>();
    };
    /// VARSIZED is the bulk-byte escape hatch: the validator in `ModelCatalog::registerModel` enforces
    /// that a VARSIZED side has exactly one field, so checking the first field is sufficient.
    const auto& modelInputs = inferModelOp.get().getModel().getSchema().inputs;
    const auto& modelOutputs = inferModelOp.get().getModel().getSchema().outputs;
    const bool varsizedInput = modelInputs.size() > 0 && modelInputs.begin()->getDataType().isType(DataType::Type::VARSIZED);
    const bool varsizedOutput = modelOutputs.size() > 0 && modelOutputs.begin()->getDataType().isType(DataType::Type::VARSIZED);
    auto physicalOperator
        = InferModelPhysicalOperator(std::move(model), toIdList(modelInputs), toIdList(modelOutputs), varsizedInput, varsizedOutput);

    NES_DEBUG("Lowering InferModel operator for model '{}' to InferModelPhysicalOperator", inferModelOp.get().getModel().getName())

    const auto memoryLayoutTypeTrait = logicalOperator.getTraitSet().tryGet<MemoryLayoutTypeTrait>();
    PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
    const auto memoryLayoutType = memoryLayoutTypeTrait.value()->memoryLayout;

    const auto physicalOutputSchema = createPhysicalOutputSchema(logicalOperator.getTraitSet());
    const auto physicalInputSchema = createPhysicalOutputSchema(inferModelOp.get().getChildren().at(0).getTraitSet());

    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator,
        physicalInputSchema,
        physicalOutputSchema,
        memoryLayoutType,
        memoryLayoutType,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    std::vector leaves(logicalOperator.getChildren().size(), wrapper);
    return {.root = wrapper, .leaves = {leaves}};
}

std::unique_ptr<AbstractLoweringRule>
LoweringRuleGeneratedRegistrar::RegisterInferModelLoweringRule(LoweringRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalInferModel>(argument.conf);
}

}
