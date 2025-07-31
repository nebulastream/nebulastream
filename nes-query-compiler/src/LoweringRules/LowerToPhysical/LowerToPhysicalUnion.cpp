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

#include <DataTypes/SchemaBase.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/SchemaFactory.hpp>
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

    const auto unionOp = logicalOperator.getAs<UnionLogicalOperator>();

    const auto traitSet = unionOp->getTraitSet();
    const auto memoryLayoutTypeTrait = logicalOperator.getTraitSet().tryGet<MemoryLayoutTypeTrait>();
    PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
    const auto memoryLayoutType = memoryLayoutTypeTrait.value()->memoryLayout;

    const auto outputSchema = createPhysicalOutputSchema(traitSet);

    auto renames = unionOp.getChildren()
        | std::views::transform(
                       [&](const auto& childOperator)
                       {
                           const auto childTraitSet = childOperator->getTraitSet();
                           const auto childOutputSchema = createPhysicalOutputSchema(childTraitSet);
                           constexpr auto extractNames = [](const Schema<QualifiedUnboundField, Ordered>& schema)
                           {
                               return schema | std::views::transform([](const auto& field) { return field.getFullyQualifiedName(); })
                                   | std::ranges::to<std::vector>();
                           };

                           return std::make_shared<PhysicalOperatorWrapper>(
                               UnionRenamePhysicalOperator{extractNames(childOutputSchema), extractNames(outputSchema)},
                               childOutputSchema,
                               outputSchema,
                               memoryLayoutType,
                               memoryLayoutType,
                               PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);
                       })
        | std::ranges::to<std::vector>();

    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        UnionPhysicalOperator(),
        outputSchema,
        outputSchema,
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
