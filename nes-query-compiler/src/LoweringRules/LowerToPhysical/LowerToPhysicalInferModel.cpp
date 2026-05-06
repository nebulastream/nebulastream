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

#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/InferModelLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/Logger/Logger.hpp>
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

    /// Compile the imported MLIR to IREE bytecode. This is where the
    /// `iree-compile` subprocess runs — deliberately deferred to lowering so
    /// the coordinator only ships the textual MLIR across the wire. The schema
    /// and signature travel with the `ImportedModel` and are propagated by
    /// `compileModel`, so no extra wiring is needed here.
    auto compiled = compileModel(inferModelOp.get().getModel().getImported());
    if (!compiled)
    {
        throw CannotLoadModel("Failed to compile model during lowering: {}", compiled.error().message);
    }
    auto model = std::move(*compiled);

    NES_DEBUG("Lowering InferModel operator to physical IREE operator (function: {})", model.getFunctionName());

    /// Create the physical operator. Input names come from the logical operator: they
    /// were resolved by `withInferredSchema` to the upstream schema's qualified names
    /// (`source$field`), which is what the runtime `record.read` lookup requires.
    /// Output names come from the model — they are the user-declared output field names
    /// that will be written back onto the record.
    /// The operator owns its own thread-local IREE session pool; no OperatorHandler needed.
    auto physicalOperator = InferModelPhysicalOperator(
        std::move(model),
        inferModelOp.get().getInputFieldNames(),
        inferModelOp.get().getOutputFieldNames(),
        inferModelOp.get().hasVarsizedInput(),
        inferModelOp.get().hasVarsizedOutput());

    const auto memoryLayoutTypeTrait = logicalOperator.getTraitSet().tryGet<MemoryLayoutTypeTrait>();
    PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
    const auto memoryLayoutType = memoryLayoutTypeTrait.value()->memoryLayout;

    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator,
        logicalOperator.getInputSchemas().at(0),
        logicalOperator.getOutputSchema(),
        memoryLayoutType,
        memoryLayoutType,
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
