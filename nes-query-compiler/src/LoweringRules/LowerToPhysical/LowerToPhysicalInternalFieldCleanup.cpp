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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalInternalFieldCleanup.hpp>

#include <memory>
#include <ranges>
#include <vector>

#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Operators/InternalFieldCleanupLogicalOperator.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/SchemaFactory.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>
#include <ScanPhysicalOperator.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalInternalFieldCleanup::apply(LogicalOperator logicalOperator)
{
    const auto cleanup = logicalOperator.getAs<InternalFieldCleanupLogicalOperator>();
    const auto traitSet = cleanup->getTraitSet();
    const auto inputSchema = createPhysicalOutputSchema(cleanup->getChild().getTraitSet());
    const auto outputSchema = createPhysicalOutputSchema(traitSet);
    const auto memoryLayout = traitSet.get<MemoryLayoutTypeTrait>()->memoryLayout;
    auto visibleFields = outputSchema | std::views::transform([](const auto& field) -> QualifiedIdentifier
                                                              { return field.getFullyQualifiedName(); })
        | std::ranges::to<std::vector>();
    auto scan = ScanPhysicalOperator(
        LowerSchemaProvider::lowerSchema(conf.pageSize.getValue(), inputSchema, memoryLayout), std::move(visibleFields));
    auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        std::move(scan),
        inputSchema,
        outputSchema,
        memoryLayout,
        memoryLayout,
        std::nullopt,
        std::nullopt,
        PhysicalOperatorWrapper::PipelineLocation::SCAN);
    return {.root = wrapper, .leaves = {wrapper}};
}

LoweringRuleRegistryReturnType
LoweringRuleGeneratedRegistrar::RegisterInternalFieldCleanupLoweringRule(LoweringRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalInternalFieldCleanup>(argument.conf);
}

}
