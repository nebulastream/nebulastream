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

#include <RewriteRules/LowerToPhysical/LowerToPhysicalProjection.hpp>

#include <memory>
#include <optional>
#include <ranges>
#include <vector>
#include <Functions/FunctionProvider.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <Util/PlanRenderer.hpp>
#include <MapPhysicalOperator.hpp>
#include <PhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>
#include <ScanPhysicalOperator.hpp>
#include <Utils.hpp>

namespace NES
{

RewriteRuleResultSubgraph LowerToPhysicalProjection::apply(LogicalOperator projectionLogicalOperator)
{
    auto projection = projectionLogicalOperator.get<ProjectionLogicalOperator>();
    auto inputSchema = projectionLogicalOperator.getInputSchemas()[0];
    auto outputSchema = projectionLogicalOperator.getOutputSchema();
    auto bufferSize = conf.pageSize.getValue();

    if (conf.layoutStrategy != MemoryLayoutStrategy::LEGACY)
    {
        auto memoryLayoutTrait = getMemoryLayoutTypeTrait(projectionLogicalOperator);
        bufferSize = conf.operatorBufferSize.getValue();
        inputSchema = inputSchema.withMemoryLayoutType(memoryLayoutTrait.targetLayoutType);
        outputSchema = outputSchema.withMemoryLayoutType(memoryLayoutTrait.targetLayoutType);
    }

    auto scanMemoryProvider = Interface::MemoryProvider::TupleBufferMemoryProvider::create(bufferSize, inputSchema);
    auto accessedFields = projection.getAccessedFields();
    auto scan = ScanPhysicalOperator(scanMemoryProvider, accessedFields);
    auto scanWrapper = std::make_shared<PhysicalOperatorWrapper>(
        scan, outputSchema, outputSchema, std::nullopt, std::nullopt, PhysicalOperatorWrapper::PipelineLocation::SCAN);

    auto child = scanWrapper;
    for (const auto& [fieldName, function] : projection.getProjections())
    {
        auto physicalFunction = QueryCompilation::FunctionProvider::lowerFunction(function);
        auto physicalOperator = MapPhysicalOperator(
            fieldName.transform([](const auto& identifier) { return identifier.getFieldName(); })
                .value_or(function.explain(ExplainVerbosity::Short)),
            physicalFunction);
        child = std::make_shared<PhysicalOperatorWrapper>(
            physicalOperator,
            outputSchema,
            outputSchema,
            std::nullopt,
            std::nullopt,
            PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE,
            std::vector{child});
    }

    if (conf.layoutStrategy != MemoryLayoutStrategy::LEGACY)
    {
        auto memoryLayoutTrait = getMemoryLayoutTypeTrait(projectionLogicalOperator);

        auto ref = addSwapBeforeOperator(scanWrapper, memoryLayoutTrait, conf);
            //addSwapBeforeProjection(scanWrapper, projectionLogicalOperator, conf);
        auto leaf = ref.second;
        /// ref.first is ignored, because childs are deeper than new swap:
        /// scan (ref.second), emit, scan (ref.first), childs
        std::vector leafes(projectionLogicalOperator.getChildren().size(), leaf);
        return {.root = child, .leafs = {leafes}};
    }

    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    const std::vector leafs(projectionLogicalOperator.getChildren().size(), child);
    return {.root = child, .leafs = {scanWrapper}};
}

std::unique_ptr<AbstractRewriteRule>
RewriteRuleGeneratedRegistrar::RegisterProjectionRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalProjection>(argument.conf);
}
}
