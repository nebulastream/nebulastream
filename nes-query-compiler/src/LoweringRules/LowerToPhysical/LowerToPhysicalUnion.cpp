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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalUnion.hpp>

#include <memory>
#include <optional>
#include <ranges>
#include <vector>
#include <DataTypes/PhysicalSchema.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>
#include <UnionPhysicalOperator.hpp>
#include <UnionRenamePhysicalOperator.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalUnion::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGetAs<UnionLogicalOperator>(), "Expected a UnionLogicalOperator");

    const auto source = logicalOperator.getAs<UnionLogicalOperator>();
    auto inputSchemas = logicalOperator.getInputSchemas();
    const auto outputPhysical = lower(logicalOperator.getOutputSchema());
    const auto memoryLayoutTypeTrait = logicalOperator.getTraitSet().tryGet<MemoryLayoutTypeTrait>();
    PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
    const auto memoryLayoutType = memoryLayoutTypeTrait.value()->memoryLayout;
    auto renames = inputSchemas
        | std::views::transform(
                       [&](const auto& schema)
                       {
                           const auto inputPhysical = lower(schema);
                           return std::make_shared<PhysicalOperatorWrapper>(
                               UnionRenamePhysicalOperator(inputPhysical.getFieldNames(), outputPhysical.getFieldNames()),
                               inputPhysical,
                               outputPhysical,
                               memoryLayoutType,
                               memoryLayoutType,
                               PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);
                       })
        | std::ranges::to<std::vector>();

    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        UnionPhysicalOperator(),
        lower(source.getOutputSchema()),
        outputPhysical,
        memoryLayoutType,
        memoryLayoutType,
        std::nullopt,
        std::nullopt,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE,
        renames);

    return {.root = wrapper, .leafs = renames};
}

LoweringRuleRegistryReturnType LoweringRuleGeneratedRegistrar::RegisterUnionLoweringRule(LoweringRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalUnion>(argument.conf);
}
}
