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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalSemMap.hpp>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <ranges>

#include <fmt/format.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SemMapLogicalOperator.hpp>
#include <Schema/Schema.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/SchemaFactory.hpp>
#include <ErrorHandling.hpp>
#include <LlmClientFactory.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>
#include <SemMapPhysicalOperator.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalSemMap::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGetAs<SemMapLogicalOperator>(), "Expected a SemMapLogicalOperator");
    const auto semMapOp = logicalOperator.getAs<SemMapLogicalOperator>();

    const auto toIdList = [](const auto& fields)
    {
        return fields
            | std::views::transform([](const UnqualifiedUnboundField& field)
                                    { return static_cast<QualifiedIdentifier>(field.getFullyQualifiedName()); })
            | std::ranges::to<std::vector>();
    };

    const auto& modelOutputs = semMapOp.get().getModel().getSchema().outputs;
    for (const auto& outputField : modelOutputs)
    {
        PRECONDITION(
            outputField.getDataType().isType(DataType::Type::VARSIZED),
            "SemMap OUTPUT field '{}' must be VARSIZED in Phase 1",
            outputField.getFullyQualifiedName());
    }

    /// Build the per-thread client factory here (worker side), capturing the config by value —
    /// the concrete clients are non-copyable/non-movable, so one instance cannot serve N worker
    /// threads. `outputBaseNames` are the catalog OUTPUT names: what the prompt asks the LLM for
    /// and what keys `SemanticMapResult` (plan §2.1).
    auto config = semMapOp.get().getModel().getConfig();
    std::vector<std::string> outputBaseNames;
    outputBaseNames.reserve(modelOutputs.size());
    for (const auto& outputField : modelOutputs)
    {
        outputBaseNames.push_back(fmt::format("{}", outputField.getFullyQualifiedName()));
    }
    LlmClientFactory factory = [config, outputBaseNames] { return createLlmClient(config, outputBaseNames); };

    /// The record fields to write are the resolved outputs — the catalog names, or the call-site
    /// alias when given — which is also what the logical output schema carries. The physical
    /// operator keeps the two apart: `modelOutputNames` key the LLM result map, the write targets
    /// address the record (plan §M4, Step 1).
    const auto resolvedOutputs = semMapOp.get().resolvedModelOutputFields();
    auto physicalOperator = SemMapPhysicalOperator(
        std::move(factory), toIdList(semMapOp.get().getCallSiteInputs()), toIdList(resolvedOutputs), outputBaseNames);

    NES_DEBUG("Lowering SemMap operator for model '{}' to SemMapPhysicalOperator", semMapOp.get().getModel().getName())

    const auto memoryLayoutTypeTrait = logicalOperator.getTraitSet().tryGet<MemoryLayoutTypeTrait>();
    PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
    const auto memoryLayoutType = memoryLayoutTypeTrait.value()->memoryLayout;

    const auto physicalOutputSchema = createPhysicalOutputSchema(logicalOperator.getTraitSet());
    const auto physicalInputSchema = createPhysicalOutputSchema(semMapOp.get().getChildren().at(0).getTraitSet());

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

}
